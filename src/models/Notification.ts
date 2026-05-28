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
      unique: true,
      index: true,
    },

    to: {
      type: String,
      maxlength: 2000,
    },

    channel: {
      type: String,
      enum: ["email", "sms", "push"],
    },

    body: {
      type: String,
      // Prevent unbounded payloads from being stored if API validation is bypassed.
      maxlength: 10000,
    },

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
