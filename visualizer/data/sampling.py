import collections
import json

with open('data.json') as data_file:
  data = json.load(data_file)
  split = int(len(data)/10)+1
  mappers = data[:split]
  mappers_to_remove = data[split:]
  data_sent_from_removed_mappers = collections.defaultdict(int)
  for m in mappers_to_remove:
    for reducerId in m["sizePerPartition"]:
      data_sent_from_removed_mappers[reducerId] += m["sizePerPartition"][reducerId]
 
  mappers.append({
    "mapperId": "fictive",
	"sizePerPartition": data_sent_from_removed_mappers
  })
  with open('sampled_data.json', 'w') as outfile:
    json.dump(mappers, outfile)
