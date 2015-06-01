map = function(doc) {
	if (doc.doctype=="collection") {
		var items = {};
		
		// For users with no items in their collection, the result will be an empty dictionary
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
					objectid = container_item[i]["@objectid"]
					
					// Many users do not have personal ratings
					if (container_item[i].stats.rating["@value"] != "N/A") {
						user_rating = container_item[i].stats.rating["@value"];
					}
					else {
						user_rating = null;
					}

					if (user_rating !== null) {
						items[objectid] = Number(user_rating);
					} else {
						items[objectid] = user_rating;
					}
				}
			}
		}
		
		emit(doc["@username"], items);
	}
}
