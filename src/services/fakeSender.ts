export async function fakeSender(to: string, channel: string, body: string) {
  console.log(`Sending ${channel} to ${to}`);
  console.log(`Message: ${body}`);

  await new Promise((resolve) => setTimeout(resolve, 2000));
  const shouldFail = Math.random() < 0.4;
  if (shouldFail) {
    throw new Error("Fake provider failure");
  }
  console.log("Notification sent successfully");
}
