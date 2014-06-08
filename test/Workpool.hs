module Main where

import Control.Monad
import Data.Binary
import Data.List (sortBy)
import Data.Function (on)
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import GHC.Packing
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import System.Exit
import System.Environment
import System.Process

usage = putStrLn "usage: workpool (master n | worker wid)"

main :: IO ()
main = do
    args <- getArgs
    case args of
      ["master", n] -> master $ read n
      ["worker", wid] -> worker wid
      _ -> usage >> exitFailure


fib :: Int -> Int
fib n = fib' !! n
    where fib' = 1 : 1 : zipWith (+) fib' (tail fib')


master :: Int -> IO ()
master n = do
    putStrLn "Master started."
    -- create socket
    sock <- socket AF_INET Stream 0
    -- make socket immediately reusable - eases debugging.
    setSocketOption sock ReuseAddr 1
    -- listen on TCP port 4242
    bindSocket sock (SockAddrInet 4242 iNADDR_ANY)
    -- allow a maximum of n outstanding connections
    listen sock n
    -- Start workers
    startWorkers n
    -- We want to compute the 20 first fibonacci numbers.
    let startList = [1..20]
        index = 0
        maxIndex = 19
        resultList = []
    masterLoop sock startList index resultList maxIndex

masterLoop :: Socket -> [Int] -> Int -> [(Int, Int)] -> Int -> IO ()
masterLoop listenSock startList index resultList maxIndex = do
    -- accept one connection and handle it
    conn <- accept listenSock
    (newIndex, newResultList) <-
        runConn conn startList index resultList
    if newIndex <= maxIndex then
        -- Continue with main loop.
        masterLoop listenSock startList newIndex
                   newResultList maxIndex
        else
            -- All calculations have been sent to the workers.
            -- All that remains is to collect the outstanding
            -- results.
            finishLoop listenSock startList newResultList

runConn :: (Socket, SockAddr) -> [Int] -> Int ->
           [(Int, Int)] -> IO (Int, [(Int, Int)])
runConn (sock, a) startList index resultList = do
  putStrLn $ "Master_runConn: Incoming connection from " ++
           (show a)
  receivedData <- recv sock 4096
  case S.unpack receivedData of
    'i':'n':'i':'t':_ -> do
      -- A worker sent an "init" message. Send the function and
      -- then immediately some data.
      sendCalculation sock
      sendData sock startList index
      -- Return the index of the next item to be sent, along with
      -- the original result list (no result was computed here).
      return (index + 1, resultList)
    _ -> do
      -- A worker sent a result. Process (store) it and
      -- then immediately send more data.
      newResultList <- processData receivedData resultList
      sendData sock startList index
      -- Return the index of the next item to be sent, along with
      -- the updated list of results.
      return (index + 1, newResultList)

finishLoop :: Socket -> [Int] -> [(Int, Int)] -> IO ()
finishLoop listenSock startList resultList = do
  -- accept one connection and handle it
  conn <- accept listenSock
  newResultList <- finishRunConn conn resultList
  if length newResultList < length startList then
      -- Still more results remaining.
      finishLoop listenSock startList newResultList
      else
          -- We have everything.
          shutDownMaster newResultList

shutDownMaster :: [(Int, Int)] -> IO ()
shutDownMaster resultList = do
  putStrLn $ "Master shutting down. The result is " ++
               (show $ sortBy (compare `on` fst) resultList)

finishRunConn :: (Socket, SockAddr) -> [(Int, Int)] ->
                 IO [(Int, Int)]
finishRunConn (sock, _) resultList = do
  receivedData <- recv sock 4096
  case S.unpack receivedData of
    'i':'n':'i':'t':_ -> do
      -- Sorry, no work here.
      sendFinish sock
      return resultList
    _ -> do
      -- Thank you and goodbye.
      newResultList <- processData receivedData resultList
      sendFinish sock
      return newResultList

sendCalculation :: Socket -> IO ()
sendCalculation sock = do
  putStrLn $ "Master_sendCalculation: Sending calculation to sock " ++ (show sock)
  -- Send the function for the worker to perform.
  let f = \x -> (x, fib x)
  clo_f <- trySerialize f
  let bin_data = L.toStrict (encode clo_f)
  sendAll sock bin_data

processData :: S.ByteString -> [(Int, Int)] -> IO [(Int, Int)]
processData receivedData resultList = do
  -- Process the result from the worker.
  fx <- deserialize $ decode $ L.fromStrict receivedData ::
        IO (Int, Int)
  -- Add the result to the front of the result list.
  return $ fx : resultList

sendData :: Socket -> [Int] -> Int -> IO ()
sendData sock startList index = do
  putStrLn $ "Master_sendData: Sending data to sock " ++ (show sock)
  -- Send an item to the worker.
  let x = startList !! index
  clo_x <- trySerialize x
  let bin_data = L.toStrict (encode clo_x)
  sendAll sock bin_data

sendFinish :: Socket -> IO ()
sendFinish sock =
    -- Send a "finished" message to the worker.
    sendAll sock $ S.pack "finished"

-- Creates <n> OS processes that run the same executable as the
-- current (with the "worker" command line argument).
startWorkers :: Int -> IO [String]
startWorkers n = do
  exePath <- getExecutablePath
  let wids = map show [1..n] -- worker IDs (strings)
  mapM (\wid -> createProcess $ proc exePath  ["worker", wid]) wids
  return wids

worker :: String -> IO ()
worker wid = do
  putStrLn $ "Worker " ++ wid ++ " started."
  response <- initWorker wid
  case response of
    Nothing -> shutDownWorker wid
    Just (sock, f) -> workerLoop wid sock f

initWorker :: String -> IO (Maybe (Socket, Int -> (Int, Int)))
initWorker wid = do
  -- Connect to the master.
  sock <- connectToMaster
  putStrLn $ "Worker_initWorker: Sending init message to master."
  -- Send "init" message to the master.
  sendAll sock $ S.pack "init"
  -- Receive data back.
  receivedData <- recv sock 4096
  case S.unpack receivedData of
    'f':'i':'n':'i':'s':'h':'e':'d':_ -> do
      -- The master immediately sent back a "finished" message.
      return Nothing
    _ -> do
      -- The master sent the function to perform.
      f <- deserialize (decode $ L.fromStrict receivedData) ::
           IO (Int -> (Int, Int))
      putStrLn $ "Worker_initWorker: Received a function from the master."
      return $ Just (sock, f)

connectToMaster :: IO Socket
connectToMaster = do
  -- Create a new socket connection to the master..
  sock <- socket AF_INET Stream 0
  addr <- inet_addr "127.0.0.1"
  connect sock $ SockAddrInet 4242 addr
  return sock

workerLoop :: String -> Socket -> (Int -> (Int, Int)) ->
              IO ()
workerLoop wid sock f = do
  -- Receive data.
  receivedData <- recv sock 4096
  -- Immediately close the connection to the master (and go work
  -- on the data received).
  sClose sock
  case S.unpack receivedData of
    'f':'i':'n':'i':'s':'h':'e':'d':_ -> do
      -- Nothing more to work on.
      putStrLn ("Worker" ++ wid ++ "_workerLoop: Received finished message from master.")
      shutDownWorker wid
    _ -> do
      -- Process the data.
      putStrLn ("Worker" ++ wid ++ "_workerLoop: Received data to work on.")
      processInput wid f receivedData

processInput :: String -> (Int -> (Int, Int)) -> S.ByteString ->
                IO ()
processInput wid f receivedData = do
  -- Process the received data.
  x <- deserialize $ decode $ L.fromStrict receivedData :: IO Int
  let fx = f x
  putStrLn $ "Worker" ++ wid ++ "_workerLoop: Calculated f(" ++ (show x) ++ ") = " ++ (show fx) ++ "."
  -- Open a new connection to the master.
  newSock <- connectToMaster
  -- Serialize and send.
  clo_fx <- trySerialize fx
  let bin_data = L.toStrict (encode clo_fx)
  sendAll newSock bin_data
  -- Continue with loop.
  workerLoop wid newSock f

shutDownWorker :: String -> IO ()
shutDownWorker wid = do
  putStrLn $ "Worker " ++ wid ++ " shutting down."
