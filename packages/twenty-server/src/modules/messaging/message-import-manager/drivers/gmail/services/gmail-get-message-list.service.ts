import { Injectable, Logger } from '@nestjs/common';

import { isNonEmptyString } from '@sniptt/guards';
import { gmail_v1 as gmailV1 } from 'googleapis';
import { isDefined } from 'twenty-shared/utils';

import { ConnectedAccountWorkspaceEntity } from 'src/modules/connected-account/standard-objects/connected-account.workspace-entity';
import { MessageChannelWorkspaceEntity } from 'src/modules/messaging/common/standard-objects/message-channel.workspace-entity';
import { MessageFolderWorkspaceEntity } from 'src/modules/messaging/common/standard-objects/message-folder.workspace-entity';
import {
  MessageImportDriverException,
  MessageImportDriverExceptionCode,
} from 'src/modules/messaging/message-import-manager/drivers/exceptions/message-import-driver.exception';
import { MESSAGING_GMAIL_EXCLUDED_CATEGORIES } from 'src/modules/messaging/message-import-manager/drivers/gmail/constants/messaging-gmail-excluded-categories';
import { MESSAGING_GMAIL_USERS_MESSAGES_LIST_MAX_RESULT } from 'src/modules/messaging/message-import-manager/drivers/gmail/constants/messaging-gmail-users-messages-list-max-result.constant';
import { GmailClientProvider } from 'src/modules/messaging/message-import-manager/drivers/gmail/providers/gmail-client.provider';
import { GmailGetHistoryService } from 'src/modules/messaging/message-import-manager/drivers/gmail/services/gmail-get-history.service';
import { GmailHandleErrorService } from 'src/modules/messaging/message-import-manager/drivers/gmail/services/gmail-handle-error.service';
import { computeGmailCategoryExcludeSearchFilter } from 'src/modules/messaging/message-import-manager/drivers/gmail/utils/compute-gmail-category-excude-search-filter.util';
import { computeGmailCategoryLabelId } from 'src/modules/messaging/message-import-manager/drivers/gmail/utils/compute-gmail-category-label-id.util';
import { mapGmailDefaultFolderToCategoryOrUndefined } from 'src/modules/messaging/message-import-manager/drivers/gmail/utils/map-gmail-default-folder-to-category';
import { GetMessageListsArgs } from 'src/modules/messaging/message-import-manager/types/get-message-lists-args.type';
import { GetMessageListsResponse } from 'src/modules/messaging/message-import-manager/types/get-message-lists-response.type';
import { assertNotNull } from 'src/utils/assert';
import { MessagingSyncCancellationService } from 'src/modules/messaging/message-import-manager/services/messaging-sync-abort.service';

@Injectable()
export class GmailGetMessageListService {
  private readonly logger = new Logger(GmailGetMessageListService.name);

  constructor(
    private readonly gmailClientProvider: GmailClientProvider,
    private readonly gmailGetHistoryService: GmailGetHistoryService,
    private readonly gmailHandleErrorService: GmailHandleErrorService,
    private readonly messagingSyncCancellationService: MessagingSyncCancellationService,
  ) {}

  private async getMessageListWithoutCursor(
    messageChannel: Pick<
      MessageChannelWorkspaceEntity,
      'loadMessagesAfterDate' | 'id'
    >,
    connectedAccount: Pick<
      ConnectedAccountWorkspaceEntity,
      'provider' | 'refreshToken' | 'id' | 'handle'
    >,
    messageFolders: Pick<MessageFolderWorkspaceEntity, 'name'>[],
  ): Promise<GetMessageListsResponse> {
    const gmailClient =
      await this.gmailClientProvider.getGmailClient(connectedAccount);

    let pageToken: string | undefined;
    let hasMoreMessages = true;

    const messageExternalIds: string[] = [];
    const excludedCategories = this.comptuteExcludedCategories(messageFolders);

    const categoryFilter =
      computeGmailCategoryExcludeSearchFilter(excludedCategories);
    const finalQuery = this.buildFinalQueryWithDateFilter(
      categoryFilter,
      messageChannel.loadMessagesAfterDate,
    );

    while (hasMoreMessages) {
      // Check if sync should be cancelled
      if (
        await this.messagingSyncCancellationService.shouldCancel(
          messageChannel.id,
        )
      ) {
        throw new MessageImportDriverException(
          `Sync cancelled for channel ${messageChannel.id}`,
          MessageImportDriverExceptionCode.SYNC_CANCELLED,
        );
      }

      const messageList = await gmailClient.users.messages
        .list({
          userId: 'me',
          maxResults: MESSAGING_GMAIL_USERS_MESSAGES_LIST_MAX_RESULT,
          pageToken,
          q: finalQuery,
        })
        .catch((error) => {
          this.gmailHandleErrorService.handleGmailMessageListFetchError(error);

          return {
            data: {
              messages: [],
              nextPageToken: undefined,
            },
          };
        });

      const { messages } = messageList.data;
      const hasMessages = messages && messages.length > 0;

      this.logger.log(
        `Fetched ${messages?.length ?? 0} messages from Gmail API.`,
      );

      if (!hasMessages) {
        break;
      }

      pageToken = messageList.data.nextPageToken ?? undefined;
      hasMoreMessages = !!pageToken;

      // @ts-expect-error legacy noImplicitAny
      messageExternalIds.push(...messages.map((message) => message.id));
    }

    if (messageExternalIds.length === 0) {
      return [
        {
          messageExternalIds,
          nextSyncCursor: '',
          previousSyncCursor: '',
          messageExternalIdsToDelete: [],
          folderId: undefined,
        },
      ];
    }

    const firstMessageExternalId = messageExternalIds[0];
    const firstMessageContent = await gmailClient.users.messages
      .get({
        userId: 'me',
        id: firstMessageExternalId,
      })
      .catch((error) => {
        this.gmailHandleErrorService.handleGmailMessagesImportError(
          error,
          firstMessageExternalId as string,
        );
      });

    const nextSyncCursor = firstMessageContent?.data?.historyId;

    if (!nextSyncCursor) {
      throw new MessageImportDriverException(
        `No historyId found for message ${firstMessageExternalId} for connected account ${connectedAccount.id}`,
        MessageImportDriverExceptionCode.NO_NEXT_SYNC_CURSOR,
      );
    }

    return [
      {
        messageExternalIds,
        nextSyncCursor,
        previousSyncCursor: '',
        messageExternalIdsToDelete: [],
        folderId: undefined,
      },
    ];
  }

  public async getMessageLists({
    messageChannel,
    connectedAccount,
    messageFolders,
  }: GetMessageListsArgs): Promise<GetMessageListsResponse> {
    const gmailClient =
      await this.gmailClientProvider.getGmailClient(connectedAccount);

    if (!isNonEmptyString(messageChannel.syncCursor)) {
      try {
        return await this.getMessageListWithoutCursor(
          messageChannel,
          connectedAccount,
          messageFolders,
        );
      } catch (error) {
        // If SYNC_CANCELLED error is thrown, return an empty list, otherwise rethrow
        if (
          error instanceof MessageImportDriverException &&
          error.code === MessageImportDriverExceptionCode.SYNC_CANCELLED
        ) {
          this.logger.log(
            `Sync cancelled for channel ${messageChannel.id}, returning empty message list`,
          );
          return [
            {
              messageExternalIds: [],
              nextSyncCursor: '',
              previousSyncCursor: '',
              messageExternalIdsToDelete: [],
              folderId: undefined,
            },
          ];
        }
        throw error;
      }
    }

    const { history, historyId: nextSyncCursor } =
      await this.gmailGetHistoryService.getHistory(
        gmailClient,
        messageChannel.syncCursor,
      );

    const { messagesAdded, messagesDeleted } =
      await this.gmailGetHistoryService.getMessageIdsFromHistory(history);

    const messageIdsToFilter = await this.getEmailIdsFromExcludedCategories(
      gmailClient,
      messageChannel.syncCursor,
      messageFolders,
    );

    const messagesAddedFiltered = messagesAdded.filter(
      (messageId) => !messageIdsToFilter.includes(messageId),
    );

    if (!nextSyncCursor) {
      throw new MessageImportDriverException(
        `No nextSyncCursor found for connected account ${connectedAccount.id}`,
        MessageImportDriverExceptionCode.NO_NEXT_SYNC_CURSOR,
      );
    }

    return [
      {
        messageExternalIds: messagesAddedFiltered,
        messageExternalIdsToDelete: messagesDeleted,
        previousSyncCursor: messageChannel.syncCursor,
        nextSyncCursor,
        folderId: undefined,
      },
    ];
  }

  private comptuteExcludedCategories(
    messageFolders: Pick<MessageFolderWorkspaceEntity, 'name'>[],
  ) {
    const includedDefaultCategories = messageFolders
      .map((messageFolder) =>
        mapGmailDefaultFolderToCategoryOrUndefined(messageFolder.name),
      )
      .filter(isDefined);

    return MESSAGING_GMAIL_EXCLUDED_CATEGORIES.filter(
      (excludedCategory) =>
        !includedDefaultCategories.includes(excludedCategory),
    );
  }

  private async getEmailIdsFromExcludedCategories(
    gmailClient: gmailV1.Gmail,
    lastSyncHistoryId: string,
    messageFolders: Pick<MessageFolderWorkspaceEntity, 'name'>[],
  ): Promise<string[]> {
    const emailIds: string[] = [];

    const excludedCategories = this.comptuteExcludedCategories(messageFolders);

    for (const category of excludedCategories) {
      const { history } = await this.gmailGetHistoryService.getHistory(
        gmailClient,
        lastSyncHistoryId,
        ['messageAdded'],
        computeGmailCategoryLabelId(category),
      );

      const emailIdsFromCategory = history
        .map((history) => history.messagesAdded)
        .flat()
        .map((message) => message?.message?.id)
        .filter((id) => id)
        .filter(assertNotNull);

      emailIds.push(...emailIdsFromCategory);
    }

    return emailIds;
  }

  /**
   * Builds the Gmail search query, applying date filter from channel
   */
  private buildFinalQueryWithDateFilter(
    categoryFilter: string,
    loadMessagesAfterDate?: string | null,
  ): string {
    let finalQuery = categoryFilter;

    if (loadMessagesAfterDate) {
      const sinceDate = new Date(loadMessagesAfterDate);
      const formattedDate = [
        sinceDate.getUTCFullYear(),
        (sinceDate.getUTCMonth() + 1).toString().padStart(2, '0'),
        sinceDate.getUTCDate().toString().padStart(2, '0'),
      ].join('/');
      finalQuery = `${categoryFilter} after:${formattedDate}`.trim();

      this.logger.log(
        `Using channel-specific loadMessagesAfterDate: ${sinceDate.toISOString()}. Gmail query: ${finalQuery}`,
      );
    } else {
      this.logger.log('No date filter applied - fetching all messages');
    }

    return finalQuery;
  }
}
