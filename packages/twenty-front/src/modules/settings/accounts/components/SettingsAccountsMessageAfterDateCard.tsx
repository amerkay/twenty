import { useState } from 'react';
import { TextInput } from '@/ui/input/components/TextInput';
import { isDefined } from 'twenty-shared/utils';
import { t } from '@lingui/core/macro';
import { format, parseISO, isAfter, startOfDay, subDays } from 'date-fns';

type SettingsLoadMessagesAfterDateCardProps = {
  value: string | null | undefined;
  onChange: (value: string | null) => void;
};

export const SettingsLoadMessagesAfterDateCard = ({
  value,
  onChange,
}: SettingsLoadMessagesAfterDateCardProps) => {
  const [error, setError] = useState<string | undefined>(undefined);

  const formatDateForInput = (
    dateString: string | null | undefined,
  ): string => {
    if (!isDefined(dateString)) return '';
    try {
      const date = parseISO(dateString);
      return format(date, 'yyyy-MM-dd');
    } catch {
      return '';
    }
  };

  const getMaxDate = (): string => {
    return format(subDays(startOfDay(new Date()), 1), 'yyyy-MM-dd');
  };

  const handleDateChange = (inputValue: string) => {
    setError(undefined); // Clear any previous error

    if (!inputValue) {
      onChange(null);
      return;
    }

    try {
      // Input value is in YYYY-MM-DD format
      const selectedDate = parseISO(inputValue);
      const maxAllowedDateString = getMaxDate();
      const maxAllowedDate = parseISO(maxAllowedDateString);

      // Prevent selecting today or future dates
      if (isAfter(selectedDate, maxAllowedDate)) {
        setError(t`Please select a date before today`);
        return; // Don't update if the date is today or in the future
      }

      onChange(inputValue); // Keep the YYYY-MM-DD format as-is
    } catch {
      setError(t`Please enter a valid date`);
    }
  };

  return (
    <TextInput
      instanceId="load-messages-after-date"
      label={t`Only import messages received on or after date`}
      placeholder={t`Select a date`}
      type="date"
      value={formatDateForInput(value)}
      onChange={handleDateChange}
      error={error}
      max={getMaxDate()}
    />
  );
};
