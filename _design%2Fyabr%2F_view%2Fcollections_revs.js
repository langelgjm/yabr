map = function(doc) {
	if (doc.doctype=="collection") {
		emit(null, doc._rev);
	}
}


