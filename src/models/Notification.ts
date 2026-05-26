import mongoose from "mongoose";

const AttemptSchema = new mongoose.Schema({
  timestamp: {
    type: Date,
    default: Date.now,
  },

  success: Boolean,

  errorMessage: String,
});

const NotificationSchema = new mongoose.Schema(
  {
    jobId: {
      type: String,
      required: true,
      index: true,
    },

    to: String,

    channel: {
      type: String,
      enum: ["email", "sms", "push"],
    },

    body: String,

    status: {
      type: String,
      enum: ["queued", "processing", "sent", "failed"],
      default: "queued",
    },

    attempts: [AttemptSchema],
  },
  {
    timestamps: true,
  },
);

export const NotificationModel = mongoose.model(
  "Notification",
  NotificationSchema,
);
