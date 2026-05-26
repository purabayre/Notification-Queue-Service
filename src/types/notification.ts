export type Channel = "email" | "sms" | "push";

export interface NotificationPayload {
  to: string;
  channel: Channel;
  body: string;
}
