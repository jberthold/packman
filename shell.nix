{ ghcVersion ? "ghc90"
, ccVersion ? "gcc9"
} :
let
  pkgs =
    import (builtins.fetchTarball {
      url = "https://github.com/NixOS/nixpkgs/archive/4ecab3273592f27479a583fb6d975d4aba3486fe.tar.gz";
      sha256 = "10wn0l08j9lgqcw8177nh2ljrnxdrpri7bp0g7nvrsn9rkawvlbf";
    }) {};

  pkgs1809 =
    import (builtins.fetchTarball {
      url = "https://github.com/NixOS/nixpkgs/archive/925ff360bc33876fdb6ff967470e34ff375ce65e.tar.gz";
      sha256 = "1qbmp6x01ika4kdc7bhqawasnpmhyl857ldz25nmq9fsmqm1vl2s";
    }) {};

  pkgs2405 =
    import (builtins.fetchTarball {
      url = "https://github.com/NixOS/nixpkgs/archive/31ac92f9628682b294026f0860e14587a09ffb4b.tar.gz";
      sha256 = "0qbyywfgjljfb4izdngxvbyvbrkilmpsmmx2a9spbwir2bcmbi14";
    }) {};

  ghc = pkgs.haskell.compiler.${ghcVersion} or pkgs1809.haskell.compiler.${ghcVersion};
in
pkgs.mkShell {
  nativeBuildInputs = [
    ghc
    pkgs.cabal-install
    pkgs.git
    pkgs2405.${ccVersion}
    pkgs.coreutils
  ];
}

