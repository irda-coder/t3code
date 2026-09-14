/**
 * DshDriver — `ProviderDriver` for the DeepSeek Harness ACP runtime.
 *
 * Phase C: snapshot/probe plus stub adapter and stub text generation. The
 * provider is visible (settings, picker, catalog) but sessions fail with
 * typed errors until Phase D lands the ACP session lifecycle.
 *
 * Two instances with different `binaryPath`/`homePath` values are fully
 * isolated: the runtime owns credentials per `DSH_HOME`, so T3 never stores
 * a DeepSeek secret.
 *
 * @module provider/Drivers/DshDriver
 */
import { DshSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerSettingsService } from "../../serverSettings.ts";
import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { makeDshTextGeneration } from "../../textGeneration/DshTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeDshAdapter } from "../Layers/DshAdapter.ts";
import {
  buildInitialDshProviderSnapshot,
  checkDshProviderStatus,
  enrichDshSnapshot,
} from "../Layers/DshProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  makeCachedProviderMaintenanceResolution,
  makeManualOnlyProviderMaintenanceCapabilities,
  type ProviderMaintenanceCapabilitiesResolver,
  resolveProviderMaintenanceCapabilitiesEffect,
} from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodeDshSettings = Schema.decodeSync(DshSettings);

const DRIVER_KIND = ProviderDriverKind.make("dsh");
// `dsh` updates itself via npm (`npm i -g @deepseek-ai/dsh`), so T3 offers no
// one-click update — manual instructions only, like Cursor's self-updater.
const UPDATE: ProviderMaintenanceCapabilitiesResolver = {
  resolve: () =>
    Effect.succeed(
      makeManualOnlyProviderMaintenanceCapabilities({
        provider: DRIVER_KIND,
        packageName: "@deepseek-ai/dsh",
      }),
    ),
};

export type DshDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | Path.Path
  | ServerSettingsService;

export const DshDriver: ProviderDriver<DshSettings, DshDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "DeepSeek",
    supportsMultipleInstances: true,
  },
  configSchema: DshSettings,
  defaultConfig: (): DshSettings => decodeDshSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const httpClient = yield* HttpClient.HttpClient;
      const serverSettings = yield* ServerSettingsService;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies DshSettings;
      const resolveMaintenance = yield* makeCachedProviderMaintenanceResolution(
        resolveProviderMaintenanceCapabilitiesEffect(UPDATE, {
          binaryPath: effectiveConfig.binaryPath,
          env: processEnv,
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
        ),
      );

      const adapter = yield* makeDshAdapter(effectiveConfig, {
        environment: processEnv,
        instanceId,
      });
      const textGeneration = yield* makeDshTextGeneration(effectiveConfig, processEnv);

      const checkProvider = checkDshProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<DshSettings>>({
        resolveMaintenance,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        checkProviderOnSettingsChange: () => false,
        refreshOnInterval: false,
        initialSnapshot: (settings) =>
          buildInitialDshProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
        enrichSnapshot: ({ settings, snapshot, publishSnapshot }) =>
          resolveMaintenance().pipe(
            Effect.flatMap((maintenanceCapabilities) =>
              enrichDshSnapshot({
                snapshot,
                maintenanceCapabilities,
                enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
                publishSnapshot,
                httpClient,
              }),
            ),
            Effect.provideService(HttpClient.HttpClient, httpClient),
          ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build DeepSeek snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
