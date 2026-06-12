// Builds the libxev backend (src/myio_xev.zig + vendored libxev) into a
// static library, zig-out/lib/libmyio_xev.a, which the Makefile links into
// the C programs. Everything else in the project builds with plain cc.
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{
        .preferred_optimize_mode = .ReleaseSafe,
    });

    const xev = b.dependency("libxev", .{
        .target = target,
        .optimize = optimize,
    }).module("xev");

    const lib = b.addLibrary(.{
        .name = "myio_xev",
        .linkage = .static,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/myio_xev.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{.{ .name = "xev", .module = xev }},
        }),
    });
    lib.bundle_compiler_rt = true;
    b.installArtifact(lib);
}
