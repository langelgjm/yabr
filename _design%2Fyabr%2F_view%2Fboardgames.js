map = function(doc) {
	if (doc.doctype=="thing") {
		if (doc["@type"] == "boardgame") {

			var name = new String();

			if (doc.name["@value"]) {
				name = doc.name["@value"];
			}
			else {
				for (var i in doc.name) {
					if (doc.name[i]["@type"] == "primary") {
						name = doc.name[i]["@value"];
						break;
					}
				}
			}
			
			var categories = new Array();
			var mechanics = new Array();
			var designers = new Array();
			var publishers = new Array();
			var families = new Array();

			for (var i in doc.link) {
				switch(doc.link[i]["@type"]) {
					case "boardgamecategory":
						categories.push(doc.link[i]["@value"]);
						break;
					case "boardgamemechanic":
						mechanics.push(doc.link[i]["@value"]);
						break;
					case "boardgamedesigner":
						designers.push(doc.link[i]["@value"]);
						break;
					case "boardgamepublisher":
						publishers.push(doc.link[i]["@value"]);
						break;
					case "boardgamefamily":
						families.push(doc.link[i]["@value"]);
						break;						
				}
			}
			
			emit(doc._id, {
				bgg_id: doc["@id"],
				name: name,
				description: doc.description,
				minplayers: doc.minplayers["@value"],
				maxplayers: doc.maxplayers["@value"],
				minplaytime: doc.minplaytime["@value"],
				playingtime: doc.playingtime["@value"],
				maxplaytime: doc.maxplaytime["@value"],
				minage: doc.minage["@value"],
				year: doc.yearpublished["@value"],
				categories: categories,
				mechanics: mechanics,
				designers: designers,
				publishers: publishers,
				families: families
			});
		}
	}
}
