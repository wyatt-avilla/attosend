{ pkgs, self }:
pkgs.testers.runNixOSTest {
  name = "attosend";
  nodes.machine =
    { ... }:
    {
      imports = [ self.nixosModules.attosend ];
      services.attosend = {
        enable = true;
        sender = "alerts@example.org";
        recipient = "test@example.org";
        credentialPath = "/etc/mock-key";
        resendUrl = "http://127.0.0.1:9088/emails";
        checks = {
          peer = "127.0.0.2";
          failureSeconds = 2;
          readiness.mock = "http://127.0.0.1:9088/ready";
        };
      };
      environment.etc."mock-key" = {
        text = "test-only-key";
        mode = "0400";
      };
      environment.systemPackages = [
        pkgs.curl
        pkgs.iproute2
      ];
      systemd = {
        services = {
          mock-resend = {
            wantedBy = [ "multi-user.target" ];
            serviceConfig = {
              ExecStart = "${pkgs.python3}/bin/python3 ${./mock_resend.py}";
              StateDirectory = "mock-resend";
            };
          };
          intentional-failure.serviceConfig = {
            Type = "oneshot";
            ExecStart = "${pkgs.coreutils}/bin/false";
          };
        };
        mounts = [
          {
            what = "none";
            where = "/badmount";
            type = "attosend-test-invalid";
          }
        ];
        timers.attosend-check.timerConfig = {
          OnBootSec = pkgs.lib.mkForce "2s";
          OnUnitActiveSec = pkgs.lib.mkForce "2s";
        };
      };
      virtualisation.memorySize = 2048;
    };
  testScript = ''
    import json
    start_all()
    machine.wait_for_unit("attosend.service")
    machine.wait_for_unit("mock-resend.service")
    machine.wait_until_succeeds("curl -fsS http://127.0.0.1:9169/metrics")

    def received(text):
        machine.wait_until_succeeds("grep -F '" + text + "' /var/lib/mock-resend/messages", timeout=120)

    with subtest("private interfaces and credentials"):
        machine.succeed("ss -ltn | grep '127.0.0.1:9169'")
        machine.fail("runuser -u nobody -- attosend queue")
        machine.fail("grep test-only-key /etc/attosend/config.json")
        machine.succeed("test $(stat -c %a /run/attosend/control.sock) = 660")

    with subtest("systemd failure, recovery, and mount hook"):
        machine.succeed("systemctl stop attosend-check.timer attosend-check.service")
        machine.fail("systemctl start intentional-failure")
        received("Unit failed: intentional-failure.service")
        machine.succeed("systemctl reset-failed intentional-failure")
        machine.succeed("systemctl start attosend-check.timer")
        received("Unit failed state cleared: intentional-failure.service")
        machine.succeed("systemctl stop attosend-check.timer attosend-check.service")
        machine.fail("systemctl start badmount.mount")
        received("Unit failed: badmount.mount")
        machine.succeed("systemctl reset-failed badmount.mount; systemctl start attosend-check.timer")

    with subtest("peer and readiness outage and recovery"):
        machine.succeed("ip route add unreachable 127.0.0.2/32 table local")
        received("FIRING: WireGuard peer unreachable")
        machine.succeed("ip route del unreachable 127.0.0.2/32 table local")
        received("RESOLVED: WireGuard peer unreachable")
        machine.succeed("touch /var/lib/mock-resend/unready")
        received("FIRING: mock is not ready")
        machine.succeed("rm /var/lib/mock-resend/unready")
        received("RESOLVED: mock is not ready")
        machine.fail("journalctl -u attosend-check -b --no-pager | grep 'Error:'")

    with subtest("queue survives restart and CLI requires daemon"):
        machine.succeed("touch /var/lib/mock-resend/reject")
        machine.succeed("attosend submit --key restart --summary restart-message")
        machine.wait_until_succeeds("attosend queue | grep failed")
        rows = json.loads(machine.succeed("attosend queue"))
        message_id = next(row["id"] for row in rows if row["state"] == "failed")
        machine.succeed("systemctl stop attosend")
        machine.succeed("test $(systemctl show attosend -p ExecMainStatus --value) = 0")
        machine.fail("attosend submit --key offline --summary offline-message")
        machine.succeed("rm /var/lib/mock-resend/reject; systemctl start attosend")
        machine.wait_until_succeeds("attosend queue")
        machine.succeed("attosend retry " + message_id)
        received("restart-message")

    with subtest("Alertmanager and metrics compatibility"):
        payload = json.dumps({"alerts": [{"fingerprint": "abc", "startsAt": "2026-01-01T00:00:00Z", "status": "firing", "labels": {"alertname": "Example"}, "annotations": {"summary": "webhook-example"}}]})
        machine.succeed("curl -fsS -H 'Content-Type: application/json' -d '" + payload + "' http://127.0.0.1:9169/alertmanager")
        received("webhook-example")
        machine.succeed("curl -fsS http://127.0.0.1:9169/metrics | grep notifications_failed")

    with subtest("handler failures do not recurse"):
        machine.succeed("mkdir -p /run/systemd/system/attosend-failure@forced.service.service.d")
        machine.succeed("printf '[Service]\\nExecStartPre=${pkgs.coreutils}/bin/false\\n' > /run/systemd/system/attosend-failure@forced.service.service.d/fail.conf")
        machine.succeed("systemctl daemon-reload")
        machine.fail("systemctl start attosend-failure@forced.service.service")
        assert machine.succeed("systemctl show attosend-failure@forced.service.service -p OnFailure --value").strip() == ""
        assert "attosend-failure@attosend-failure" not in machine.succeed("systemctl list-units --all --no-pager")
  '';
}
