{
  inputs = {
    nixpkgs.url = "github:cachix/devenv-nixpkgs/rolling";
    systems.url = "github:nix-systems/default";
    devenv.url = "github:cachix/devenv";
    devenv.inputs.nixpkgs.follows = "nixpkgs";
  };

  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs =
    {
      self,
      nixpkgs,
      devenv,
      systems,
      ...
    }@inputs:
    let
      forEachSystem = nixpkgs.lib.genAttrs (import systems);
    in
    {
      devShells = forEachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = devenv.lib.mkShell {
            inherit inputs pkgs;
            modules = [ ./devenv.nix ];
          };
        }
      );

      packages = forEachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        rec {
          underclass = pkgs.rustPlatform.buildRustPackage {
            pname = "underclass";
            version = "0.1.0";

            src = self;
            cargoLock.lockFile = ./Cargo.lock;
            doCheck = false;
            nativeBuildInputs = [ pkgs.makeWrapper ];
            postInstall = ''
              makeWrapper "$out/bin/underclass" "$out/bin/utop" --add-flags top
            '';

            meta = with pkgs.lib; {
              description = "Pooled multi-subscription ChatGPT/Codex and GitHub Copilot proxy with an OpenAI-compatible endpoint";
              homepage = "https://github.com/ghuntley/underclass";
              license = licenses.mit;
              mainProgram = "underclass";
            };
          };
          default = underclass;
        }
      );

      apps = forEachSystem (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.underclass}/bin/underclass";
        };
        utop = {
          type = "app";
          program = "${self.packages.${system}.underclass}/bin/utop";
        };
      });

      overlays.default = final: _prev: {
        underclass = self.packages.${final.stdenv.hostPlatform.system}.underclass;
      };

      nixosModules.default = import ./nixos/module.nix self;

      checks = forEachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          isLinux = nixpkgs.lib.elem system [
            "x86_64-linux"
            "aarch64-linux"
          ];
        in
        nixpkgs.lib.optionalAttrs isLinux {
          underclass-module = pkgs.runCommand "underclass-nixos-module-eval" {
            toplevel =
              (pkgs.nixos [
                self.nixosModules.default
                {
                  services.underclass.enable = true;
                  system.stateVersion = "25.11";
                  fileSystems."/".device = "none";
                  fileSystems."/".fsType = "tmpfs";
                  boot.loader.grub.enable = false;
                }
              ]).config.system.build.toplevel.drvPath;
          } "test -n \"$toplevel\" && touch $out";

          underclass-vm = import ./nixos/tests/underclass.nix {
            inherit pkgs;
            module = self.nixosModules.default;
          };
        }
      );
    };
}
