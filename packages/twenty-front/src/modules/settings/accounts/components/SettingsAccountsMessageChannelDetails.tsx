import styled from '@emotion/styled';

import {
  MessageChannel,
  MessageChannelContactAutoCreationPolicy,
} from '@/accounts/types/MessageChannel';
import { CoreObjectNameSingular } from '@/object-metadata/types/CoreObjectNameSingular';
import { useUpdateOneRecord } from '@/object-record/hooks/useUpdateOneRecord';
import { SettingsLoadMessagesAfterDateCard } from '@/settings/accounts/components/SettingsAccountsMessageAfterDateCard';
import { SettingsAccountsMessageAutoCreationCard } from '@/settings/accounts/components/SettingsAccountsMessageAutoCreationCard';
import { SettingsAccountsMessageVisibilityCard } from '@/settings/accounts/components/SettingsAccountsMessageVisibilityCard';
import { SettingsOptionCardContentToggle } from '@/settings/components/SettingsOptions/SettingsOptionCardContentToggle';
import { MessageChannelVisibility } from '~/generated-metadata/graphql';
import { t } from '@lingui/core/macro';
import { Card, Section } from 'twenty-ui/layout';
import { H2Title, IconBriefcase, IconUsers } from 'twenty-ui/display';

type SettingsAccountsMessageChannelDetailsProps = {
  messageChannel: Pick<
    MessageChannel,
    | 'id'
    | 'visibility'
    | 'contactAutoCreationPolicy'
    | 'excludeNonProfessionalEmails'
    | 'excludeGroupEmails'
    | 'loadMessagesAfterDate'
    | 'isSyncEnabled'
  >;
};

const StyledDetailsContainer = styled.div`
  display: flex;
  flex-direction: column;
  gap: ${({ theme }) => theme.spacing(6)};
`;

export const SettingsAccountsMessageChannelDetails = ({
  messageChannel,
}: SettingsAccountsMessageChannelDetailsProps) => {
  const { updateOneRecord } = useUpdateOneRecord<MessageChannel>({
    objectNameSingular: CoreObjectNameSingular.MessageChannel,
  });

  const handleVisibilityChange = (value: MessageChannelVisibility) => {
    updateOneRecord({
      idToUpdate: messageChannel.id,
      updateOneRecordInput: {
        visibility: value,
      },
    });
  };

  const handleContactAutoCreationChange = (
    value: MessageChannelContactAutoCreationPolicy,
  ) => {
    updateOneRecord({
      idToUpdate: messageChannel.id,
      updateOneRecordInput: {
        contactAutoCreationPolicy: value,
      },
    });
  };

  const handleIsGroupEmailExcludedToggle = (value: boolean) => {
    updateOneRecord({
      idToUpdate: messageChannel.id,
      updateOneRecordInput: {
        excludeGroupEmails: value,
      },
    });
  };

  const handleIsNonProfessionalEmailExcludedToggle = (value: boolean) => {
    updateOneRecord({
      idToUpdate: messageChannel.id,
      updateOneRecordInput: {
        excludeNonProfessionalEmails: value,
      },
    });
  };

  const handleLoadMessagesAfterDateChange = (value: string | null) => {
    updateOneRecord({
      idToUpdate: messageChannel.id,
      updateOneRecordInput: {
        loadMessagesAfterDate: value,
      },
    });
  };

  return (
    <StyledDetailsContainer>
      <Section>
        <H2Title
          title={t`Visibility`}
          description={t`Define what will be visible to other users in your workspace`}
        />
        <SettingsAccountsMessageVisibilityCard
          value={messageChannel.visibility}
          onChange={handleVisibilityChange}
        />
      </Section>
      <Section>
        <H2Title
          title={t`Contact auto-creation`}
          description={t`Automatically create People records when receiving or sending emails`}
        />
        <SettingsAccountsMessageAutoCreationCard
          value={messageChannel.contactAutoCreationPolicy}
          onChange={handleContactAutoCreationChange}
        />
      </Section>
      <Section>
        <Card rounded>
          <SettingsOptionCardContentToggle
            Icon={IconBriefcase}
            title={t`Exclude non-professional emails`}
            description={t`Don’t create contacts from/to Gmail, Outlook emails`}
            divider
            checked={messageChannel.excludeNonProfessionalEmails}
            onChange={() => {
              handleIsNonProfessionalEmailExcludedToggle(
                !messageChannel.excludeNonProfessionalEmails,
              );
            }}
          />
          <SettingsOptionCardContentToggle
            Icon={IconUsers}
            title={t`Exclude group emails`}
            description={t`Don’t sync emails from team@ support@ noreply@...`}
            checked={messageChannel.excludeGroupEmails}
            onChange={() =>
              handleIsGroupEmailExcludedToggle(
                !messageChannel.excludeGroupEmails,
              )
            }
          />
        </Card>
      </Section>
      <Section>
        <H2Title
          title={t`Import Filters`}
          description={t`Configure which messages to import from your email account`}
        />
        <SettingsLoadMessagesAfterDateCard
          value={messageChannel.loadMessagesAfterDate}
          onChange={handleLoadMessagesAfterDateChange}
        />
      </Section>
    </StyledDetailsContainer>
  );
};
