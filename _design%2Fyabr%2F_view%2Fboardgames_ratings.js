reduce = function(keys, values) {
	// First set output equal to stats from first element in values
	var average = values[0].average;
	var bayesaverage = values[0].bayesaverage;
	var median = values[0].median;
	var stddev = values[0].stddev;
	var usersrated = values[0].usersrated;

	// Check each value to see if it has a higher usersrated (and thus is later)
	// If so, use those stats instead
	for (var i in values) {
		if (values[i].usersrated > usersrated) {
			usersrated = values[i].usersrated;
			average = values[i].average;
			bayesaverage = values[i].bayesaverage;
			median = values[i].median;
			stddev = values[i].stddev;
			usersrated = values[i].usersrated;
		}
	}

	var rating = {
		average: average,
		bayesaverage: bayesaverage,
		median: median,
		stddev: stddev,
		usersrated: usersrated
	}
	return rating;
}
map = function(doc) {
	if (doc.doctype=="collection") {
		
		// For users with no items in their collection, we want no output
		if (+doc["@totalitems"] > 0) {
		
			// For users with only one item, we create a container_item that is an array with one element
			// This allows us to use the same logic as with > 1 item
			if (+doc["@totalitems"] == 1) {
				var container_item = new Array();
				container_item.push(doc.item);
			}
			else {
				container_item = doc.item;
			}
	
			// Include only things that are boardgames; add each as a dictionary to the items dictionary
			for (var i in container_item) {
			
				if (container_item[i]["@objecttype"] == "thing" && container_item[i]["@subtype"] == "boardgame") {
					// The thing's objectid will serve as the key for the dictionary
					var objectid = Number(container_item[i]["@objectid"]);
					
					var rating = {
						average: Number(container_item[i].stats.rating.average["@value"]),
						bayesaverage: Number(container_item[i].stats.rating.bayesaverage["@value"]),
						median: Number(container_item[i].stats.rating.median["@value"]),
						stddev: Number(container_item[i].stats.rating.stddev["@value"]),
						usersrated: Number(container_item[i].stats.rating.usersrated["@value"])
					};
					
					emit(objectid, rating);
				}
			}
		}
	}
}

