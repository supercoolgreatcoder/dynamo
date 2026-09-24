# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

{ facadePath
, agentgatewayPath
, envoyPath
, envoyModulePath
, version ? "dev"
}:
let
  pkgs = import (builtins.getFlake "nixpkgs") { system = "x86_64-linux"; };
  runtimeLibs = pkgs.lib.makeLibraryPath [
    pkgs.stdenv.cc.cc.lib
    pkgs.glibc
    pkgs.openssl
    pkgs.zeromq
    pkgs.zlib
  ];
in
pkgs.runCommand "dynamo-component-pipelines-${version}" {
  nativeBuildInputs = [ pkgs.patchelf ];
} ''
  mkdir -p $out/bin $out/lib
  cp ${facadePath} $out/bin/dynamo-component-facade
  cp ${agentgatewayPath} $out/bin/agentgateway
  cp ${envoyPath} $out/bin/envoy-static
  cp ${envoyModulePath} $out/lib/libgeneric_pipeline.so
  chmod +w $out/bin/* $out/lib/*
  for binary in $out/bin/*; do
    patchelf --set-interpreter ${pkgs.glibc}/lib/ld-linux-x86-64.so.2 \
      --set-rpath "${runtimeLibs}" "$binary"
  done
  patchelf --set-rpath "${runtimeLibs}" $out/lib/libgeneric_pipeline.so
  chmod -w $out/bin/* $out/lib/*
''
