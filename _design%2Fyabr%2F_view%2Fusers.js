reduce = function(keys, values) {
	return sum(values);
}

map = function(doc) {
	if (doc.doctype=="thread") {
		// If this key exists, it's a single article thread
		if (doc.articles.article["@username"]) {
			emit(doc.articles.article["@username"], 1);
		}
		// For multi-article threads, repeat for each article
		else {
			for (var i in doc.articles.article) {
				emit(doc.articles.article[i]["@username"], 1);
			}
		}
	}
}
