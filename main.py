# Step 1: Find the ""batch generated"" in Leader Node. 
# Step 2: Need to get the Batch ID of that batch.
# Step 3: Record the time stamp of the batch generation of that Batch.
# Step 4: Find the ""BATCH_CHUNK_CREATION_AT_LEADER"" in Leader Node.
# Step 5: Need to get same Batch ID of the batch that got in the batch generated.
# Step 6: Record the time stamp of the batch chunk creation at node of that Batch ID.
# Step 7: Need to substract the ""BATCH_CHUNK_CREATION_AT_LEADER"" and ""batch generated"" time stamp to the get Difference."

# BATCH_CHUNK_CREATION_AT_LEADER: 
# {"msg":"BATCH_CHUNK_CREATION_AT_LEADER","time":"2022-03-31T06:40:51.675635491Z","target":"batch::types","line":156,"file":"batch/src/types/mod.rs","clan_type":"LeaderClan(0)","tribe_id":"0","clan_id":"0","batch_chunk_creation_time":"1648708851675","batch_chunk_creation_length":"25","node_id":"6","batch_chunk_data_size":"152","node_type":"Leader(0)","batch_id":"799cf35f83a72546b64aefc869d12e6b10bf73ddc6bf8d6fde5ae84fca4d71","timestamp":"1648708851675"}

# BATCH_CHUNK_SHARED_TO_LEADER_CLAN_MEMBER:
# {"msg":"BATCH_CHUNK_SHARED_TO_LEADER_CLAN_MEMBER","time":"2022-03-31T06:40:41.276851059Z","target":"supra_core::broadcast","line":237,"file":"app/src/broadcast/mod.rs","timestamp":"1648708841276","batch_id":"15bd748b4cbb37d4c2251ef3bd9a428039af057c1131235be528e06b5b","node_id":"6","tribe_id":"0","node_type":"Leader(0)","clan_id":"0","clan_type":"LeaderClan(0)","batch_chunk_index":"19"}

# BATCH_CHUNK_RECEIVED_AT_SAME_CLAN_MEMBER
# {"msg":"BATCH_CHUNK_RECEIVED_AT_SAME_CLAN_MEMBER","tribe_id":"0","clan_type":"LeaderClan(1)","node_type":"Basic","batch_chunk_index":"12","time_difference":"1209","clan_id":"1","node_id":"12","timestamp":"1648708842230","batch_id":"12212d5808853b8c08f5bc20a4dd2d2f75ff81bafb11d7d071ac7cde5e52d9"}
import os
import json

ROOT_FOLDER = "/home/codezeros/Samay-Test/465/logs"


log_files = os.listdir(ROOT_FOLDER)


BATCH_CHUNK_CREATION_TIMES = []

def get_batches():

	batches = [];

	for log in log_files:
		path_to_log = ROOT_FOLDER+ "/" + log
		line_counter = 0
		with open(path_to_log) as f:
			for line in f:
				line_counter +=1
				if "BATCH_CREATED" in line:
					# print("BATCH_CREATED!");
					# print("log name: {}".format(log))

					batch_data = json.loads(line)
					batch_data["chunks"] = {}
					batch_data["chunks_counter"] = 0

					# from batch created to chunk created
					batch_data["chunk_creation_time_minus_batch_creation_time"] = []
					batch_data["chunk_creation_time_minus_batch_creation_time_avg"] = 0

					# from batch created to chunk sent to leader nodes
					batch_data["chunk_sent_leader_clan_time_minus_batch_creation_time"] = []
					batch_data["chunk_sent_leader_clan_time_minus_batch_creation_time_avg"] = 0					

					# from batch created to chunk sent to other nodes
					batch_data["chunk_sent_other_clan_time_minus_batch_creation_time"] = []
					batch_data["chunk_sent_other_clan_time_minus_batch_creation_time_avg"] = 0

					# from chunk sent to same leader clan to received
					batch_data["chunk_received_at_same_leader_clan"] = []
					batch_data["chunk_received_at_same_leader_clan_avg"] = 0

					# from chunk sent to same other clan to received
					batch_data["chunk_received_at_other_clan"] = []
					batch_data["chunk_received_at_other_clan_avg"] = 0

					# batch regenerated at nodes
					batch_data["time_for_batch_regenerated_at_node"] = []
					batch_data["time_for_batch_regenerated_at_node_avg"] = 0

					# print("find BATCH_CREATED: {}".format(batch_data))
					with open(path_to_log) as f2:
						for line2 in f2:
							# print("find BATCH_CHUNK_CREATION_AT_LEADER")
							if "BATCH_CHUNK_CREATION_AT_LEADER" in line2:

								chunk_data = json.loads(line2)
								if chunk_data["batch_id"]==batch_data["batch_id"]:
									chunk_creation_time_minus_batch_creation_time = int(chunk_data["timestamp"]) - int(batch_data["timestamp"]);
									batch_data["chunk_creation_time_minus_batch_creation_time"].append({
										"time": chunk_creation_time_minus_batch_creation_time,
										"tribe_id": chunk_data["tribe_id"],
										"clan_id": chunk_data["clan_id"],
										"chunk_hash": chunk_data["batch_chunk_hash"],
										"node_id": chunk_data["node_id"]
									})

							if "BATCH_CHUNK_SHARED_TO_LEADER_CLAN_MEMBER" in line2:
								chunk_data = json.loads(line2)
								if chunk_data["batch_id"]==batch_data["batch_id"]:
									batch_data["chunks_counter"]  +=1
									batch_chunk_index = chunk_data["batch_chunk_index"]
									chunk_data["type_of_sent"] = "leader";
									batch_data["chunks"][batch_chunk_index] = chunk_data;
									chunk_sent_leader_clan_time_minus_batch_creation_time = int(chunk_data["timestamp"]) - int(batch_data["timestamp"]);
									batch_data["chunk_sent_leader_clan_time_minus_batch_creation_time"].append({
										"time": chunk_sent_leader_clan_time_minus_batch_creation_time,
										"tribe_id": chunk_data["tribe_id"],
										"clan_id": chunk_data["clan_id"],
										"chunk_hash": chunk_data["batch_chunk_hash"],
										"node_id": chunk_data["node_id"]
									})
									# batch_chunk_index

							if "BATCH_CHUNK_SHARED_TO_OTHER_CLAN_MEMBER" in line2:
								chunk_data = json.loads(line2)
								if chunk_data["batch_id"]==batch_data["batch_id"]:
									batch_data["chunks_counter"]  +=1
									batch_chunk_index = chunk_data["batch_chunk_index"]
									chunk_data["type_of_sent"] = "other";
									batch_data["chunks"][batch_chunk_index] = chunk_data;
									chunk_sent_other_clan_time_minus_batch_creation_time = int(chunk_data["timestamp"]) - int(batch_data["timestamp"]);
									batch_data["chunk_sent_other_clan_time_minus_batch_creation_time"].append({
										"time": chunk_sent_other_clan_time_minus_batch_creation_time,
										"tribe_id": chunk_data["tribe_id"],
										"clan_id": chunk_data["clan_id"],
										"chunk_hash": chunk_data["batch_chunk_hash"],
										"node_id": chunk_data["node_id"]
									})
					sum1 = round(sum(item['time'] for item in batch_data["chunk_creation_time_minus_batch_creation_time"]))
					sum2 = round(sum(item['time'] for item in batch_data["chunk_sent_leader_clan_time_minus_batch_creation_time"]))
					sum3 = round(sum(item['time'] for item in batch_data["chunk_sent_other_clan_time_minus_batch_creation_time"]))

					batch_data["chunk_creation_time_minus_batch_creation_time_avg"] = sum1 / len(batch_data["chunk_creation_time_minus_batch_creation_time"])
					batch_data["chunk_sent_leader_clan_time_minus_batch_creation_time_avg"] = sum2 / len(batch_data["chunk_sent_leader_clan_time_minus_batch_creation_time"])
					batch_data["chunk_sent_other_clan_time_minus_batch_creation_time_avg"] = sum3 / len(batch_data["chunk_sent_other_clan_time_minus_batch_creation_time"])

					batches.append(batch_data)
	return batches

def check_batches_chunk_at_destination(batches):
	for log in log_files:
		path_to_log = ROOT_FOLDER+ "/" + log
		with open(path_to_log) as f:

			for line in f:
				if "BATCH_CHUNK_RECEIVED_AT_SAME_CLAN_MEMBER" in line:
					chunk_data = json.loads(line)
					for batch_data in batches:
						if batch_data["batch_id"]==chunk_data["batch_id"]:
							chunks_indexes = batch_data["chunks"].keys()
							chunk_batch_index = chunk_data["batch_chunk_index"]
							if chunk_batch_index in chunks_indexes:
								chunk_sent_data = batch_data["chunks"][chunk_batch_index]
								chunk_recieved_at_same_clan_minus_chunk_sent_time = int(chunk_data["timestamp"]) - int(chunk_sent_data["timestamp"])
								batch_data["chunk_received_at_same_leader_clan"].append({
										"time": chunk_recieved_at_same_clan_minus_chunk_sent_time,
										"tribe_id": chunk_data["tribe_id"],
										"clan_id": chunk_data["clan_id"],
										"chunk_hash": chunk_data["batch_chunk_hash"],
										"node_id": chunk_data["node_id"]
									})
							else:
								print("ERROR? chunk batch_chunk_index ({}) not found on batch data: {}".format(chunk_batch_index, chunks_indexes))

				if "BATCH_CHUNK_RECEIVED_AT_OTHER_CLAN_MEMBER" in line:
					chunk_data = json.loads(line)
					for batch_data in batches:
						if batch_data["batch_id"]==chunk_data["batch_id"]:
							chunks_indexes = batch_data["chunks"].keys()
							chunk_batch_index = chunk_data["batch_chunk_index"]
							if chunk_batch_index in chunks_indexes:
								chunk_sent_data = batch_data["chunks"][chunk_batch_index]
								chunk_recieved_at_other_clan_minus_chunk_sent_time = int(chunk_data["timestamp"]) - int(chunk_sent_data["timestamp"])
								batch_data["chunk_received_at_other_clan"].append({
										"time": chunk_recieved_at_other_clan_minus_chunk_sent_time,
										"tribe_id": chunk_data["tribe_id"],
										"clan_id": chunk_data["clan_id"],
										"chunk_hash": chunk_data["batch_chunk_hash"],
										"node_id": chunk_data["node_id"]
									})
							else:
								print("ERROR? chunk batch_chunk_index ({}) not found on batch data: {}".format(chunk_batch_index, chunks_indexes))

				if "BATCH_REGENERATE_IN_NODES" in line:
					batch_data2 = json.loads(line)
					for batch_data in batches:
						if batch_data["batch_id"]==batch_data2["batch_id"]:
							time_for_batch_to_regenerate = int(batch_data2["timestamp"]) - int(batch_data["timestamp"])
							# print("!!!!!!! BATCH_REGENERATE_IN_NODES batch_id:{}, time: {}".format(batch_data["batch_id"], time_for_batch_to_regenerate))
							batch_data["time_for_batch_regenerated_at_node"].append({
										"time": time_for_batch_to_regenerate,
										"tribe_id": batch_data2["tribe_id"],
										"clan_id": batch_data2["clan_id"],
										"batch_id": batch_data2["batch_id"],
										"node_id": batch_data2["node_id"]
									});
	# print("batches: {}".format(batches))
	return batches


def main():
	batches = get_batches()
	print("1. len of batches {}".format(len(batches)))
	batches = check_batches_chunk_at_destination(batches)
	print("2. len of batches {}".format(len(batches)))
	for batch_data in batches:
		sum1 = round(sum(item['time'] for item in batch_data["chunk_received_at_same_leader_clan"]))
		sum2 = round(sum(item['time'] for item in batch_data["chunk_received_at_other_clan"]))		
		sum3 = round(sum(item['time'] for item in batch_data["time_for_batch_regenerated_at_node"]))
		batch_data["chunk_received_at_same_leader_clan_avg"] = sum1 / len(batch_data["chunk_received_at_same_leader_clan"])
		batch_data["chunk_received_at_other_clan_avg"] = sum2 / len(batch_data["chunk_received_at_other_clan"])
		batch_data["time_for_batch_regenerated_at_node_avg"] = sum3 / len(batch_data["time_for_batch_regenerated_at_node"])

		print("batch id {}".format(batch_data["batch_id"]))
		print("chunks number: {}".format(batch_data["chunks_counter"]))
		print("avg time for batch to regenerate: {}".format(batch_data["time_for_batch_regenerated_at_node_avg"]))
		print("avg time from batch create to chunk create: {}".format(batch_data["chunk_creation_time_minus_batch_creation_time_avg"]))
		print("avg time from batch create to chunk sent to leader: {}".format(batch_data["chunk_sent_leader_clan_time_minus_batch_creation_time_avg"]))
		print("avg time from batch create to chunk sent to other: {}".format(batch_data["chunk_sent_other_clan_time_minus_batch_creation_time_avg"]))
		print("avg time from chunk sent to chunk received. same clan: {}".format(batch_data["chunk_received_at_same_leader_clan_avg"]))
		print("avg time from chunk sent to chunk received. other clan: {}".format(batch_data["chunk_received_at_other_clan_avg"]))
	return batches
# main()