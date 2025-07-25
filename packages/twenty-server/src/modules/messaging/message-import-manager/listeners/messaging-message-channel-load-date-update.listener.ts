import { Injectable, Logger, Scope } from '@nestjs/common';

import { ObjectRecordUpdateEvent } from 'src/engine/core-modules/event-emitter/types/object-record-update.event';
import { objectRecordChangedProperties } from 'src/engine/core-modules/event-emitter/utils/object-record-changed-properties.util';
import { WorkspaceEventBatch } from 'src/engine/workspace-event-emitter/types/workspace-event.type';
import { MessageChannelWorkspaceEntity } from 'src/modules/messaging/common/standard-objects/message-channel.workspace-entity';
import { OnDatabaseBatchEvent } from 'src/engine/api/graphql/graphql-query-runner/decorators/on-database-batch-event.decorator';
import { OnCustomBatchEvent } from 'src/engine/api/graphql/graphql-query-runner/decorators/on-custom-batch-event.decorator';
import { DatabaseEventAction } from 'src/engine/api/graphql/graphql-query-runner/enums/database-event-action';
import { MessagingSyncCancellationService } from 'src/modules/messaging/message-import-manager/services/messaging-sync-abort.service';
import { MESSAGING_SYNC_CANCELLED_EVENT } from 'src/modules/messaging/message-import-manager/constants/messaging-sync-cancelled-event.constant';
import { InjectMessageQueue } from 'src/engine/core-modules/message-queue/decorators/message-queue.decorator';
import { MessageQueue } from 'src/engine/core-modules/message-queue/message-queue.constants';
import { MessageQueueService } from 'src/engine/core-modules/message-queue/services/message-queue.service';
import {
  MessagingMessageListFetchJob,
  MessagingMessageListFetchJobData,
} from 'src/modules/messaging/message-import-manager/jobs/messaging-message-list-fetch.job';

@Injectable()
export class MessagingMessageChannelLoadDateUpdateListener {
  private readonly logger = new Logger(
    MessagingMessageChannelLoadDateUpdateListener.name,
  );

  constructor(
    private readonly messagingSyncCancellationService: MessagingSyncCancellationService,
    @InjectMessageQueue(MessageQueue.messagingQueue)
    private readonly messageQueueService: MessageQueueService,
  ) {}

  @OnDatabaseBatchEvent('messageChannel', DatabaseEventAction.UPDATED)
  async handleUpdatedEvent(
    payload: WorkspaceEventBatch<
      ObjectRecordUpdateEvent<MessageChannelWorkspaceEntity>
    >,
  ) {
    const channelsWithDateChange = payload.events.filter((eventPayload) =>
      objectRecordChangedProperties(
        eventPayload.properties.before,
        eventPayload.properties.after,
      ).includes('loadMessagesAfterDate'),
    );

    if (channelsWithDateChange.length === 0) {
      return;
    }

    channelsWithDateChange.forEach((event) => {
      const before = event.properties.before.loadMessagesAfterDate;
      const after = event.properties.after.loadMessagesAfterDate;
      this.logger.log(`Channel ${event.recordId}: ${before} → ${after}`);
    });

    // Request cancellation for ongoing syncs - jobs will be enqueued when cancellation completes
    for (const event of channelsWithDateChange) {
      await this.messagingSyncCancellationService.requestCancellation(
        event.recordId,
        payload.workspaceId,
      );
      this.logger.log(
        `Requested cancellation for channel ${event.recordId} due to loadMessagesAfterDate change`,
      );
    }
  }

  @OnCustomBatchEvent(MESSAGING_SYNC_CANCELLED_EVENT)
  async handleSyncCancelled(
    payload: WorkspaceEventBatch<{ messageChannelId: string }>,
  ) {
    for (const event of payload.events) {
      await this.messageQueueService.add<MessagingMessageListFetchJobData>(
        MessagingMessageListFetchJob.name,
        {
          workspaceId: payload.workspaceId,
          messageChannelId: event.messageChannelId,
        },
      );

      this.logger.log(
        `Successfully enqueued job for channel ${event.messageChannelId}`,
      );
    }
  }
}
