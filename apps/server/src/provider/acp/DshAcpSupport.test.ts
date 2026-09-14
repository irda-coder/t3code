import { describe, expect, it } from "@effect/vitest";

import {
  buildDshAcpSpawnInput,
  decodeDshModelOptionValue,
  DSH_ACP_MODEL_CONFIG_ID,
  encodeDshModelOptionValue,
  resolveDshModelRoute,
} from "./DshAcpSupport.ts";
import { dshModelSlugForRoute } from "../Layers/DshAdapter.ts";

describe("resolveDshModelRoute", () => {
  it("falls back to the official flash route for empty input", () => {
    expect(resolveDshModelRoute(undefined)).toEqual({
      provider: "deepseek-official",
      model: "deepseek-v4-flash",
    });
    expect(resolveDshModelRoute("   ")).toEqual({
      provider: "deepseek-official",
      model: "deepseek-v4-flash",
    });
  });

  it("treats a bare slug as an official-route model", () => {
    expect(resolveDshModelRoute("deepseek-v4-pro")).toEqual({
      provider: "deepseek-official",
      model: "deepseek-v4-pro",
    });
  });

  it("splits provider/model routes on the first slash", () => {
    expect(resolveDshModelRoute("openrouter/inclusionai/ling-3.0-flash-vl:free")).toEqual({
      provider: "openrouter",
      model: "inclusionai/ling-3.0-flash-vl:free",
    });
  });

  it("trims whitespace around the route", () => {
    expect(resolveDshModelRoute("  openrouter / model-x  ")).toEqual({
      provider: "openrouter",
      model: "model-x",
    });
  });
});

describe("encodeDshModelOptionValue / decodeDshModelOptionValue", () => {
  it("round-trips a route through the ACP model option value", () => {
    const route = { provider: "openrouter", model: "some-model" };
    expect(decodeDshModelOptionValue(encodeDshModelOptionValue(route))).toEqual(route);
    expect(DSH_ACP_MODEL_CONFIG_ID).toBe("model");
  });

  it("rejects malformed option values", () => {
    expect(decodeDshModelOptionValue(undefined)).toBeUndefined();
    expect(decodeDshModelOptionValue("")).toBeUndefined();
    expect(decodeDshModelOptionValue("not-json")).toBeUndefined();
    expect(decodeDshModelOptionValue('["only-one"]')).toBeUndefined();
    expect(decodeDshModelOptionValue('["", "model"]')).toBeUndefined();
    expect(decodeDshModelOptionValue('{"provider":"x"}')).toBeUndefined();
  });
});

describe("dshModelSlugForRoute", () => {
  it("uses the bare model for the official route", () => {
    expect(
      dshModelSlugForRoute({ provider: "deepseek-official", model: "deepseek-v4-flash" }),
    ).toBe("deepseek-v4-flash");
  });

  it("prefixes third-party routes with the provider", () => {
    expect(dshModelSlugForRoute({ provider: "openrouter", model: "some-model" })).toBe(
      "openrouter/some-model",
    );
  });
});

describe("buildDshAcpSpawnInput", () => {
  it("defaults to the dsh binary with the acp profile", () => {
    expect(buildDshAcpSpawnInput(undefined, "/tmp/project")).toEqual({
      command: "dsh",
      args: ["--profile", "acp"],
      cwd: "/tmp/project",
    });
  });

  it("honors custom binary, profile, home and environment", () => {
    expect(
      buildDshAcpSpawnInput(
        { binaryPath: "/usr/local/bin/dsh", profile: "acp", homePath: "/tmp/dsh-home" },
        "/tmp/project",
        { OPENROUTER_API_KEY: "secret" },
      ),
    ).toEqual({
      command: "/usr/local/bin/dsh",
      args: ["--profile", "acp"],
      cwd: "/tmp/project",
      env: {
        OPENROUTER_API_KEY: "secret",
        DSH_HOME: "/tmp/dsh-home",
      },
    });
  });
});
