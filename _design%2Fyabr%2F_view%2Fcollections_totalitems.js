map = function(doc) {
	if (doc.doctype=="collection") {
		emit(doc["@username"], +doc["@totalitems"]);
	}
}

