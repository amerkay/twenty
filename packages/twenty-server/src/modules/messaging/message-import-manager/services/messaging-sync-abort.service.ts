import { Injectable, Logger } from '@nestjs/common';

import { InjectCacheStorage } from 'src/engine/core-modules/cache-storage/decorators/cache-storage.decorator';
import { CacheStorageService } from 'src/engine/core-modules/cache-storage/services/cache-storage.service';
import { CacheStorageNamespace } from 'src/engine/core-modules/cache-storage/types/cache-storage-namespace.enum';
import { WorkspaceEventEmitter } from 'src/engine/workspace-event-emitter/workspace-event-emitter';
import { MESSAGING_SYNC_CANCELLATION_TTL } from 'src/modules/messaging/message-import-manager/constants/messaging-sync-cancellation-ttl.constant';
import { MESSAGING_SYNC_CANCELLED_EVENT } from 'src/modules/messaging/message-import-manager/constants/messaging-sync-cancelled-event.constant';

@Injectable()
export class MessagingSyncCancellationService {
  private readonly logger = new Logger(MessagingSyncCancellationService.name);

  constructor(
    @InjectCacheStorage(CacheStorageNamespace.ModuleMessaging)
    private readonly cacheStorage: CacheStorageService,
    private readonly workspaceEventEmitter: WorkspaceEventEmitter,
  ) {}

  async requestCancellation(
    messageChannelId: string,
    workspaceId: string,
  ): Promise<void> {
    const key = `sync-cancel:${messageChannelId}`;
    const cancellationData = {
      shouldCancel: true,
      workspaceId: workspaceId,
    };
    await this.cacheStorage.set(
      key,
      cancellationData,
      MESSAGING_SYNC_CANCELLATION_TTL,
    );
    this.logger.log(
      `Requested sync cancellation for channel ${messageChannelId}`,
    );
  }

  async shouldCancel(messageChannelId: string): Promise<boolean> {
    const key = `sync-cancel:${messageChannelId}`;
    const cancellationData = await this.cacheStorage.get<{
      shouldCancel: boolean;
      workspaceId: string;
    }>(key);

    if (cancellationData?.shouldCancel) {
      await this.clearCancellation(messageChannelId);

      this.workspaceEventEmitter.emitCustomBatchEvent(
        MESSAGING_SYNC_CANCELLED_EVENT,
        [{ messageChannelId }],
        cancellationData.workspaceId,
      );

      this.logger.log(
        `Sync cancelled and event emitted for channel ${messageChannelId}`,
      );
      return true;
    }

    return false;
  }

  async clearCancellation(messageChannelId: string): Promise<void> {
    const key = `sync-cancel:${messageChannelId}`;
    await this.cacheStorage.del(key);
  }
}
