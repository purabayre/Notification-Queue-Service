// Channel identifiers supported by this service.
export type Channel = "email" | "sms" | "push";

// Kept for documentation/typing purposes.
// Used by API input validation and job payloads.
export type NotificationPayload = {
  to: string;
  channel: Channel;
  body: string;
};
