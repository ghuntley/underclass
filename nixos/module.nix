self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.underclass;
  inherit (lib)
    mkEnableOption
    mkIf
    mkOption
    types
    literalExpression
    ;

  configFile = pkgs.writeText "underclass-config.toml" (
    lib.concatStringsSep "\n" (
      lib.mapAttrsToList (name: value: "${name} = ${builtins.toJSON value}") cfg.settings
    )
  );
  configDirectory = pkgs.runCommand "underclass-config" { } ''
    mkdir "$out"
    cp ${configFile} "$out/config.toml"
  '';

  bindMatch = builtins.match "^(.+):([0-9]+)$" cfg.bindAddress;
  port = if bindMatch == null then 0 else lib.toInt (builtins.elemAt bindMatch 1);
in
{
  options.services.underclass = {
    enable = mkEnableOption "underclass, the pooled ChatGPT/Codex + Copilot subscription proxy";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.underclass;
      defaultText = literalExpression "underclass flake package";
      description = "The underclass package to run.";
    };

    bindAddress = mkOption {
      type = types.str;
      default = "127.0.0.1:8080";
      description = "Address and port the proxy listens on (`host:port`).";
    };

    stateDirectory = mkOption {
      type = types.strMatching "^[A-Za-z0-9_.-]+$";
      default = "underclass";
      description = ''
        systemd StateDirectory name. Credentials, sticky bindings, the model
        catalog, and minted keys are persisted to /var/lib/<name>/pool.db.
      '';
    };

    openFirewall = mkOption {
      type = types.bool;
      default = false;
      description = "Open the firewall for the bind port. Only needed when exposing the proxy beyond localhost.";
    };

    environmentFile = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "/run/secrets/underclass.env";
      description = ''
        Path to an environment file loaded by the service, for secrets:
        UNDERCLASS_PROXY_KEY and UNDERCLASS_UI_TOKEN (underclass mints both
        when absent). Use this instead of putting secrets into the Nix store.
      '';
    };

    settings = mkOption {
      type = types.attrsOf (
        types.oneOf [
          types.bool
          types.int
          types.float
          types.str
        ]
      );
      default = { };
      example = lib.literalExpression ''
        {
          codex_cooldown_secs = 1800;
          copilot_cooldown_secs = 1800;
        }
      '';
      description = ''
        Non-secret options written to a generated config.toml
        (bind lives in `services.underclass.bindAddress`; keep secrets in
        `services.underclass.environmentFile`).
      '';
    };
  };

  config = mkIf cfg.enable {
    environment.systemPackages = [ cfg.package ];

    assertions = [
      {
        assertion = bindMatch != null && port > 0 && port <= 65535;
        message = "services.underclass.bindAddress must be of the form host:port with a port from 1 to 65535";
      }
    ];

    networking.firewall.allowedTCPPorts = mkIf cfg.openFirewall [ port ];

    systemd.services.underclass = {
      description = "underclass pooled subscription proxy";
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      wantedBy = [ "multi-user.target" ];

      environment = {
        UNDERCLASS_DATA_DIR = "/var/lib/${cfg.stateDirectory}";
        UNDERCLASS_CONFIG_DIR = "${configDirectory}";
      };

      serviceConfig = {
        ExecStart = "${cfg.package}/bin/underclass serve --bind ${cfg.bindAddress}";
        EnvironmentFile = mkIf (cfg.environmentFile != null) [ (toString cfg.environmentFile) ];

        DynamicUser = true;
        StateDirectory = cfg.stateDirectory;
        Restart = "on-failure";
        RestartSec = 5;

        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        PrivateDevices = true;
        ProtectKernelTunables = true;
        ProtectKernelModules = true;
        ProtectControlGroups = true;
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
        ];
        RestrictNamespaces = true;
        LockPersonality = true;
        MemoryDenyWriteExecute = true;
        SystemCallArchitectures = "native";
        UMask = "0077";
      };
    };
  };
}
