{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-parts.follows = "baskingspot/flake-parts";
    baskingspot = {
      url = "github:baskingspot/nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      # imports and inputsFrom below mirror each other, including the
      # toolchain version: pick that once, in the import.
      imports = with inputs.baskingspot.flakeModules; [
        base
        docs
        zig."0.17"
      ];

      perSystem =
        { pkgs, config, ... }:
        let
          # The conformance runner is not in nixpkgs. Pinned by the release's
          # own checksums so the dev shell and CI run one binary.
          conformanceVersion = "1.0.7";
          conformanceAsset =
            {
              "aarch64-linux" = "Linux-aarch64";
              "x86_64-linux" = "Linux-x86_64";
              "aarch64-darwin" = "Darwin-arm64";
            }
            .${pkgs.stdenv.hostPlatform.system};
          conformanceHashes = {
            "Linux-aarch64" = "c43e11cdca4cf7936a502fa25e1d7f27a6475b806d0ab5647668a5777b9d2121";
            "Linux-x86_64" = "cf2d7c671c8cfbdfba5e48dc58cbe0b2db01fa7b1c160ecf060f20d27479d63c";
            "Darwin-arm64" = "6e3534f8a39ad91843913af80fa7f68fdc266a71560b00ad078a0637d380100b";
          };
          connectconformance = pkgs.stdenv.mkDerivation {
            pname = "connectconformance";
            version = conformanceVersion;
            src = pkgs.fetchurl {
              url = "https://github.com/connectrpc/conformance/releases/download/v${conformanceVersion}/connectconformance-v${conformanceVersion}-${conformanceAsset}.tar.gz";
              sha256 = conformanceHashes.${conformanceAsset};
            };
            sourceRoot = ".";
            installPhase = ''
              install -Dm755 connectconformance $out/bin/connectconformance
            '';
          };
        in
        {
          packages.connectconformance = connectconformance;

          devShells.default = pkgs.mkShell {
            inputsFrom = [
              config.devShells.base
              config.devShells.docs
              config.devShells."zig-0-17"
            ];

            # Project-specific tools.
            packages = with pkgs; [
              # `just demo-etcd` starts a throwaway etcd.
              etcd
              # `just conformance-tls` mints the certificate pair the runner
              # hands the client.
              mkcert
              # The TLS client resolves libcrypto through pkg-config.
              openssl.dev
              openssl.out
              pkg-config
              # `just conformance` drives the client under test.
              connectconformance
            ];
          };
        };
    };
}
