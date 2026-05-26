import mongoose from "mongoose";
import dotenv from "dotenv";

dotenv.config();

export async function connectMongo() {
  await mongoose.connect(process.env.MONGO_URI as string);

  console.log("MongoDB connected");
}
