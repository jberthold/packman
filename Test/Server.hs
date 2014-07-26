module Main where

import Control.Monad
import Data.Binary
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import GHC.Packing
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import System.Exit
import System.Environment

usage = putStrLn "usage: test-server (server|client)"

main :: IO ()
main = do
    args <- getArgs
    case args of
      ["server"] -> server
      ["client"] -> client
      _ -> usage >> exitFailure


fib :: [Int]
fib = 1 : 1 : zipWith (+) fib (tail fib)


server = do
    -- create socket
    sock <- socket AF_INET Stream 0
    -- make socket immediately reusable - eases debugging.
    setSocketOption sock ReuseAddr 1
    -- listen on TCP port 4242
    bindSocket sock (SockAddrInet 4242 iNADDR_ANY)
    -- allow a maximum of 2 outstanding connections
    listen sock 2
    mainLoop sock

mainLoop :: Socket -> IO ()
mainLoop sock = do
    -- accept one connection and handle it
    conn <- accept sock
    runConn conn
    mainLoop sock

runConn :: (Socket, SockAddr) -> IO ()
runConn (sock, _) = do
    clo <- liftM L.fromStrict $ recv sock 5012
    fib <- deserialize (decode clo) :: IO [Int]
    sendAll sock $ S.pack (show (fib !! 100))
    sClose sock

client = do
    -- create socket
    sock <- socket AF_INET Stream 0
    addr <- inet_addr "127.0.0.1"
    connect sock $ SockAddrInet 4242 addr
    clo_fib <- trySerialize fib
    let bin_data = L.toStrict (encode clo_fib)
    sendAll sock bin_data
    msg <- recv sock 1024
    sClose sock
    S.putStrLn msg

