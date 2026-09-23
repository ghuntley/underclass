{
  pkgs,
  module,
}:
pkgs.testers.nixosTest {
  name = "underclass-service";

  nodes.machine =
    { ... }:
    {
      imports = [ module ];
      services.underclass = {
        enable = true;
        environmentFile = "/etc/underclass-test.env";
      };
      environment.etc."underclass-test.env".text = ''
        UNDERCLASS_PROXY_KEY=test-key
        UNDERCLASS_UI_TOKEN=test-ui-token
      '';
      environment.systemPackages = [ pkgs.curl ];
    };

  testScript = ''
    machine.start()
    machine.wait_for_unit("underclass.service")
    machine.wait_for_open_port(8080)

    with subtest("service diagnostics do not expose credentials"):
        machine.fail("journalctl -u underclass.service --no-pager | grep -F test-key")
        machine.fail("journalctl -u underclass.service --no-pager | grep -F test-ui-token")

    with subtest("web ui is served"):
        machine.succeed("curl -sf http://127.0.0.1:8080/ | grep -qi underclass")

    with subtest("v1 endpoints reject missing proxy key"):
        machine.fail("curl -sf -o /dev/null http://127.0.0.1:8080/v1/models")
        machine.fail("curl -sf -o /dev/null -X POST http://127.0.0.1:8080/v1/responses")

    with subtest("admin endpoints require the UI token"):
        machine.fail("curl -sf -o /dev/null http://127.0.0.1:8080/admin/api/state")
        machine.fail("curl -sf -o /dev/null http://127.0.0.1:8080/admin/api/monitor")
        machine.succeed(
            "curl -sf -H 'Authorization: Bearer test-ui-token' "
            "http://127.0.0.1:8080/admin/api/state | grep -q accounts"
        )

    with subtest("local monitor socket needs no token and exposes no admin controls"):
        machine.succeed("test -S /run/underclass/monitor.sock")
        machine.succeed("test $(stat -c %a /run/underclass) = 755")
        machine.succeed("test $(stat -c %a /run/underclass/monitor.sock) = 666")
        machine.succeed("curl --unix-socket /run/underclass/monitor.sock -sf http://localhost/monitor | grep -q minute_bins")
        machine.fail("curl --unix-socket /run/underclass/monitor.sock -sf -o /dev/null http://localhost/admin/api/state")

    with subtest("v1 models serves the union catalog with the proxy key"):
        machine.succeed(
            "curl -sf -H 'Authorization: Bearer test-key' http://127.0.0.1:8080/v1/models | grep -q gpt-5.5"
        )

    with subtest("state persists to the state directory"):
        machine.succeed("test -f /var/lib/underclass/pool.db")

    with subtest("service survives a restart with its persisted state"):
        machine.systemctl("restart underclass")
        machine.wait_for_unit("underclass.service")
        machine.wait_for_open_port(8080)
        machine.succeed(
            "curl -sf -H 'Authorization: Bearer test-key' http://127.0.0.1:8080/v1/models | grep -q gpt-5.5"
        )

    with subtest("empty pool reports no configured subscriptions"):
        machine.succeed(
            "curl -s -o /tmp/response -w '%{http_code}\\n' -X POST "
            "-H 'Authorization: Bearer test-key' -H 'Content-Type: application/json' "
            "-d '{\"model\":\"gpt-5.5\",\"input\":\"hi\"}' "
            "http://127.0.0.1:8080/v1/responses | tee /dev/stderr | grep -qx 503"
        )
        machine.succeed("grep -q no_accounts /tmp/response")
  '';
}
