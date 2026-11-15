{ pkgs, rustToolchain }:
pkgs.mkShell {
  LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
  packages = with pkgs; [
    clang
    cmake
    ffmpeg
    nixfmt
    rustToolchain
  ];
}
