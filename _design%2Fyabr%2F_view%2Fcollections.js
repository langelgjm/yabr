map = function(doc) {
	if (doc.doctype=="collection") {
		var items = new Array();
		
		// For users with no items in their collection, the result will be an empty list
		if (+doc["@totalitems"] > 0) {
			// For users with only one item, we create a container_item that is an array with one element
			if (+doc["@totalitems"] == 1) {
				var container_item = new Array();
				container_item.push(doc.item);
			}
			else {
				container_item = doc.item;
			}
	
			// Include only things that are boardgames; push them onto the items array
			for (var i in container_item) {
				if (container_item[i]["@objecttype"] == "thing" && container_item[i]["@subtype"] == "boardgame") {
					item = {
							collid: container_item[i]["@collid"],
							objectid: container_item[i]["@objectid"],
							name: container_item[i].name["#text"],
							numplays: container_item[i].numplays,
							stats: container_item[i].stats,
							status: container_item[i].status,
							yearpublished: container_item[i].yearpublished
							};
					items.push(item);
				}
			}
		}
		
		emit(doc["@username"], items);
	}
}
