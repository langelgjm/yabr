map = function(doc) {
	if (doc.doctype=="thread") {
		if (doc["@id"] && doc["@numarticles"]) {
			emit(+doc["@id"], +doc["@numarticles"]);
		}
		else {
			emit(doc._id, -1)
		}
	}
}

