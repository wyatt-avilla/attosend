{
  description = "Durable local incident notifications through Resend";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    nix-checks = {
      url = "github:wyatt-avilla/nix-ci";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    { self, nixpkgs, ... }@inputs:
    {
      nixosModules.attosend = import ./service.nix { inherit self; };
      nixosModules.default = self.nixosModules.attosend;
    }
    // inputs.flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import inputs.rust-overlay) ];
        };
        toolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [
            "clippy"
            "rustfmt"
            "rust-src"
            "rust-analyzer"
          ];
        };
        rustPlatform = pkgs.makeRustPlatform {
          cargo = toolchain;
          rustc = toolchain;
        };
        cargoToml = fromTOML (builtins.readFile ./Cargo.toml);
        formatter = pkgs.writeShellApplication {
          name = "treefmt";
          runtimeInputs = [
            pkgs.nixfmt-rfc-style
            pkgs.prettier
            toolchain
          ];
          text = ''
            exec ${pkgs.treefmt}/bin/treefmt "$@"
          '';
        };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            toolchain
            formatter
            nixfmt-rfc-style
            statix
            deadnix
          ];
        };

        inherit formatter;

        packages.default = rustPlatform.buildRustPackage {
          pname = cargoToml.package.name;
          inherit (cargoToml.package) version;
          src = pkgs.lib.fileset.toSource {
            root = ./.;
            fileset = pkgs.lib.fileset.unions [
              ./Cargo.toml
              ./Cargo.lock
              ./src
              ./migrations
              (pkgs.lib.fileset.fileFilter (file: file.hasExt "rs") ./tests)
            ];
          };
          cargoLock.lockFile = ./Cargo.lock;
          nativeBuildInputs = [
            toolchain
            pkgs.makeWrapper
          ];
          postInstall = pkgs.lib.optionalString pkgs.stdenv.isLinux ''
            wrapProgram "$out/bin/attosend" --prefix PATH : ${
              pkgs.lib.makeBinPath [
                pkgs.systemd
                pkgs.iputils
              ]
            }
          '';
          checkPhase = ''
            runHook preCheck
            cargo fmt --all --check
            cargo clippy --locked --all-targets -- -W clippy::pedantic -D warnings
            cargo test --locked --all-targets
            runHook postCheck
          '';
          meta.mainProgram = cargoToml.package.name;
        };

        checks =
          pkgs.lib.optionalAttrs (system == "x86_64-linux") {
            nixos-vm = import ./tests/vm.nix { inherit pkgs self; };
          }
          // {
            package = self.packages.${system}.default;
            formatting = pkgs.runCommand "check-formatting" { nativeBuildInputs = [ formatter ]; } ''
              cp -r ${self} source
              chmod -R u+w source
              cd source
              export HOME="$TMPDIR"
              treefmt --ci --walk filesystem
              mkdir "$out"
            '';
            linting = inputs.nix-checks.lib.mkLintingCheck {
              inherit pkgs;
              src = self;
            };
            dead-code = pkgs.runCommandLocal "check-dead-code" { nativeBuildInputs = [ pkgs.deadnix ]; } ''
              deadnix --fail ${self}
              mkdir "$out"
            '';
          };
      }
    );
}
