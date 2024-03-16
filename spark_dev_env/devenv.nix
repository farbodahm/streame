{ pkgs, ... }:

let
  python-packages = p: with p; [ pip python-lsp-server importmagic epc black ];
in {
  # https://devenv.sh/basics/
  env.GREET = "🛠️ Let's hack 🧑🏻‍💻";

  # # https://devenv.sh/packages/
  packages = with pkgs; [
    kcat
    ruff
    stdenv.cc.cc.lib # required by jupyter
    gcc-unwrapped # fix: libstdc++.so.6: cannot open shared object file
    (python3.withPackages python-packages)
  ];

  env.LD_LIBRARY_PATH = "${pkgs.gcc-unwrapped.lib}/lib64";

  # https://devenv.sh/scripts/
  scripts.hello.exec = "echo $GREET";

  enterShell = ''
    hello
  '';

  # https://devenv.sh/languages/
  languages.python = {
    enable = true;
    package = pkgs.python310;
    poetry = {
      enable = true;
      activate.enable = true;
      install.enable = true;
      install.allExtras = true;
      # install.groups = [ "dev" "test" "infra" ];
    };
  };

  languages.java.enable = true;
  languages.java.jdk.package = pkgs.jdk11;

  # Make diffs fantastic
  difftastic.enable = true;

  # https://devenv.sh/pre-commit-hooks/
  pre-commit.hooks = {
    black.enable = true;
    ruff.enable = true;
    nixfmt.enable = true;
    yamllint.enable = true;
  };

  # Enable dotenv
  dotenv.enable = true;
  devcontainer.enable = true;
}
