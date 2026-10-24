{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.attosend;
  format = pkgs.formats.json { };
  settings = format.generate "attosend.json" {
    inherit (cfg)
      sender
      recipient
      host
      port
      reminderSeconds
      resendUrl
      logLevel
      ;
    database = "/var/lib/attosend/queue.db";
    socket = "/run/attosend/control.sock";
    checks = {
      inherit (cfg.checks) peer readiness failureSeconds;
      systemdFailures = cfg.systemdFailures.enable;
    };
  };
  command = "${lib.getExe cfg.package} --config /etc/attosend/config.json";
  hardening = {
    User = "attosend";
    Group = "attosend";
    UMask = "0027";
    NoNewPrivileges = true;
    ProtectSystem = "strict";
    ProtectHome = true;
    PrivateTmp = true;
    RestrictAddressFamilies = [
      "AF_UNIX"
      "AF_INET"
      "AF_INET6"
    ];
  };
  unitTypes = [
    "service"
    "socket"
    "timer"
    "path"
    "mount"
    "automount"
    "swap"
    "target"
    "scope"
  ];
in
{
  options.services.attosend = {
    enable = lib.mkEnableOption "durable local incident notifications";
    package = lib.mkOption {
      type = lib.types.package;
      inherit (self.packages.${pkgs.stdenv.hostPlatform.system}) default;
      description = "Attosend package to run.";
    };
    sender = lib.mkOption {
      type = lib.types.str;
      description = "Verified Resend sender address.";
    };
    recipient = lib.mkOption {
      type = lib.types.str;
      description = "Notification recipient address.";
    };
    credentialPath = lib.mkOption {
      type = lib.types.str;
      description = "Runtime path to a file containing the Resend API key; never put its contents in the Nix store.";
    };
    host = lib.mkOption {
      type = lib.types.str;
      default = config.networking.hostName;
      description = "Host label in emails.";
    };
    port = lib.mkOption {
      type = lib.types.port;
      default = 9169;
      description = "Loopback Alertmanager and metrics port.";
    };
    reminderSeconds = lib.mkOption {
      type = lib.types.ints.positive;
      default = 21600;
      description = "Minimum interval between repeated firing notifications.";
    };
    resendUrl = lib.mkOption {
      type = lib.types.str;
      default = "https://api.resend.com/emails";
      description = "Resend endpoint; override for local tests.";
    };
    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "info";
      description = "Tracing filter for journal logs.";
    };
    systemdFailures.enable = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Install global failure hooks and poll failed units.";
    };
    checks = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Run independent host checks every 30 seconds.";
      };
      peer = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Peer IP address or hostname to ping.";
      };
      readiness = lib.mkOption {
        type = lib.types.attrsOf lib.types.str;
        default = { };
        description = "Named HTTP readiness URLs; status 200 means healthy.";
      };
      failureSeconds = lib.mkOption {
        type = lib.types.ints.positive;
        default = 120;
        description = "Observed outage duration before notifying.";
      };
    };
  };
  config = lib.mkIf cfg.enable {
    users.users.attosend = {
      isSystemUser = true;
      group = "attosend";
    };
    users.groups.attosend = { };
    environment.systemPackages = [ cfg.package ];
    environment.etc."attosend/config.json".source = settings;
    systemd = {
      services = {
        attosend = {
          description = "Durable notification delivery and Alertmanager receiver";
          wantedBy = [ "multi-user.target" ];
          wants = [ "network-online.target" ];
          after = [ "network-online.target" ];
          serviceConfig = hardening // {
            ExecStart = "${command} serve";
            ExecStartPost = pkgs.writeShellScript "attosend-ready" ''
              until ${command} queue >/dev/null 2>&1; do
                ${pkgs.coreutils}/bin/sleep 0.1
              done
            '';
            TimeoutStartSec = "60s";
            LoadCredential = [ "resend-api-key:${cfg.credentialPath}" ];
            StateDirectory = "attosend";
            StateDirectoryMode = "0750";
            RuntimeDirectory = "attosend";
            RuntimeDirectoryMode = "0750";
            Restart = "always";
            RestartSec = "5s";
            TimeoutStopSec = "35s";
          };
        };
      }
      // lib.optionalAttrs cfg.systemdFailures.enable {
        "attosend-failure@" = {
          description = "Queue notification for failed unit %i";
          serviceConfig = hardening // {
            Type = "oneshot";
            ExecStart = "${command} unit-failure -- %i";
            TimeoutStartSec = "45s";
          };
        };
      }
      // lib.optionalAttrs cfg.checks.enable {
        attosend-check = {
          description = "Check peer, readiness endpoints, and failed units";
          after = [
            "network.target"
            "attosend.service"
          ];
          serviceConfig = hardening // {
            Type = "oneshot";
            ExecStart = "${command} check";
            TimeoutStartSec = "60s";
            AmbientCapabilities = lib.optional (cfg.checks.peer != null) "CAP_NET_RAW";
            CapabilityBoundingSet = lib.optional (cfg.checks.peer != null) "CAP_NET_RAW";
          };
        };
      };
      timers.attosend-check = lib.mkIf cfg.checks.enable {
        wantedBy = [ "timers.target" ];
        timerConfig = {
          OnBootSec = "30s";
          OnUnitActiveSec = "30s";
          AccuracySec = "1s";
        };
      };
      packages = lib.mkIf cfg.systemdFailures.enable [
        (pkgs.runCommand "attosend-failure-dropins" { } ''
          for type in ${lib.concatStringsSep " " unitTypes}; do
            mkdir -p "$out/lib/systemd/system/$type.d"
            printf '[Unit]\nOnFailure=attosend-failure@%%n.service\n' > "$out/lib/systemd/system/$type.d/50-attosend.conf"
          done
          mkdir -p "$out/lib/systemd/system/attosend-failure@.service.d"
          ln -s /dev/null "$out/lib/systemd/system/attosend-failure@.service.d/50-attosend.conf"
        '')
      ];
    };
  };
}
