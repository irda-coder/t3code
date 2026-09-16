import { describe, expect, it } from "@effect/vitest";

import { piSlugForModelRef, resolvePiModelRef } from "./PiProvider.ts";

describe("resolvePiModelRef", () => {
  it("returns undefined for the pi-default sentinel", () => {
    expect(resolvePiModelRef("pi-default")).toBeUndefined();
  });

  it("returns undefined for empty input", () => {
    expect(resolvePiModelRef(undefined)).toBeUndefined();
    expect(resolvePiModelRef(null)).toBeUndefined();
    expect(resolvePiModelRef("   ")).toBeUndefined();
  });

  it("splits provider/id slugs", () => {
    expect(resolvePiModelRef("openrouter/inclusionai/ling-3.0-flash-vl:free")).toEqual({
      provider: "openrouter",
      model: "inclusionai/ling-3.0-flash-vl:free",
    });
  });

  it("trims whitespace around both parts", () => {
    expect(resolvePiModelRef("  openrouter / model-x  ")).toEqual({
      provider: "openrouter",
      model: "model-x",
    });
  });

  it("rejects bare slugs and malformed routes", () => {
    expect(resolvePiModelRef("bare-slug")).toBeUndefined();
    expect(resolvePiModelRef("/model")).toBeUndefined();
    expect(resolvePiModelRef("provider/")).toBeUndefined();
    expect(resolvePiModelRef("provider/   ")).toBeUndefined();
  });
});

describe("piSlugForModelRef", () => {
  it("encodes the sentinel for undefined", () => {
    expect(piSlugForModelRef(undefined)).toBe("pi-default");
  });

  it("round-trips provider/id refs", () => {
    const ref = { provider: "openrouter", model: "model-x" };
    expect(resolvePiModelRef(piSlugForModelRef(ref))).toEqual(ref);
  });
});
