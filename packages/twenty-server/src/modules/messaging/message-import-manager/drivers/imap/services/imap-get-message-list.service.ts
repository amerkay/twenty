import { Injectable, Logger } from '@nestjs/common';

import { ImapFlow } from 'imapflow';

import { ImapClientProvider } from 'src/modules/messaging/message-import-manager/drivers/imap/providers/imap-client.provider';
import { ImapHandleErrorService } from 'src/modules/messaging/message-import-manager/drivers/imap/services/imap-handle-error.service';
import { findSentMailbox } from 'src/modules/messaging/message-import-manager/drivers/imap/utils/find-sent-mailbox.util';
import { MessagingSyncCancellationService } from 'src/modules/messaging/message-import-manager/services/messaging-sync-abort.service';
import { GetMessageListsArgs } from 'src/modules/messaging/message-import-manager/types/get-message-lists-args.type';
import { GetMessageListsResponse } from 'src/modules/messaging/message-import-manager/types/get-message-lists-response.type';
import {
  MessageImportDriverException,
  MessageImportDriverExceptionCode,
} from 'src/modules/messaging/message-import-manager/drivers/exceptions/message-import-driver.exception';

@Injectable()
export class ImapGetMessageListService {
  private readonly logger = new Logger(ImapGetMessageListService.name);

  constructor(
    private readonly imapClientProvider: ImapClientProvider,
    private readonly imapHandleErrorService: ImapHandleErrorService,
    private readonly messagingSyncCancellationService: MessagingSyncCancellationService,
  ) {}

  async getMessageLists({
    messageChannel,
    connectedAccount,
  }: GetMessageListsArgs): Promise<GetMessageListsResponse> {
    try {
      const client = await this.imapClientProvider.getClient(connectedAccount);

      const mailboxes = ['INBOX'];

      const sentFolder = await findSentMailbox(client, this.logger);

      if (sentFolder) {
        mailboxes.push(sentFolder);
      }

      let allMessages: { id: string; date: string }[] = [];

      try {
        for (const mailbox of mailboxes) {
          const messages = await this.getMessagesFromMailbox(
            client,
            mailbox,
            messageChannel.syncCursor,
            messageChannel.loadMessagesAfterDate,
            messageChannel.id,
          );

          allMessages = [...allMessages, ...messages];
          this.logger.log(
            `Fetched ${messages.length} messages from ${mailbox}`,
          );
        }
      } catch (error) {
        if (
          error instanceof MessageImportDriverException &&
          error.code === MessageImportDriverExceptionCode.SYNC_CANCELLED
        ) {
          this.logger.log(
            `Sync cancelled for channel ${messageChannel.id}, returning empty message list`,
          );
          // Discard partially fetched messages
          allMessages = [];
        } else {
          throw error;
        }
      }

      allMessages.sort(
        (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime(),
      );

      const messageExternalIds = allMessages.map((message) => message.id);

      const nextSyncCursor =
        allMessages.length > 0
          ? allMessages[allMessages.length - 1].date
          : messageChannel.syncCursor || '';

      return [
        {
          messageExternalIds,
          nextSyncCursor,
          previousSyncCursor: messageChannel.syncCursor,
          messageExternalIdsToDelete: [],
          folderId: undefined,
        },
      ];
    } catch (error) {
      this.imapHandleErrorService.handleImapMessageListFetchError(error);

      return [
        {
          messageExternalIds: [],
          nextSyncCursor: messageChannel.syncCursor || '',
          previousSyncCursor: messageChannel.syncCursor,
          messageExternalIdsToDelete: [],
          folderId: undefined,
        },
      ];
    } finally {
      await this.imapClientProvider.closeClient(connectedAccount.id);
    }
  }

  private async getMessagesFromMailbox(
    client: ImapFlow,
    mailbox: string,
    cursor?: string,
    loadMessagesAfterDate?: string | null,
    messageChannelId?: string,
  ): Promise<{ id: string; date: string }[]> {
    let lock;

    try {
      lock = await client.getMailboxLock(mailbox);

      let searchOptions: { since?: Date } = {};
      if (loadMessagesAfterDate) {
        searchOptions.since = new Date(loadMessagesAfterDate);
      } else if (cursor) {
        searchOptions.since = new Date(cursor);
      }

      const messages: { id: string; date: string }[] = [];

      for await (const message of client.fetch(searchOptions, {
        envelope: true,
      })) {
        if (
          messageChannelId &&
          (await this.messagingSyncCancellationService.shouldCancel(
            messageChannelId,
          ))
        ) {
          throw new MessageImportDriverException(
            `Sync cancelled for channel ${messageChannelId}`,
            MessageImportDriverExceptionCode.SYNC_CANCELLED,
          );
        }

        if (message.envelope?.messageId) {
          const messageDate = message.envelope.date
            ? new Date(message.envelope.date)
            : new Date();
          const validDate = isNaN(messageDate.getTime())
            ? new Date()
            : messageDate;

          messages.push({
            id: message.envelope.messageId,
            date: validDate.toISOString(),
          });
        }
      }

      return messages;
    } catch (error) {
      if (
        error instanceof MessageImportDriverException &&
        error.code === MessageImportDriverExceptionCode.SYNC_CANCELLED
      ) {
        throw error;
      }

      this.logger.error(
        `Error fetching from mailbox ${mailbox}: ${error.message}`,
        error.stack,
      );

      return [];
    } finally {
      if (lock) {
        lock.release();
      }
    }
  }
}
