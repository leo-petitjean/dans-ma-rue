const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    // TODO Compter le nombre d'anomalies par arondissement
        client.search({
            index: 'test_dans_ma_rue',
            body: {
                aggs: {
                    "arrondissement": {
                        "histogram": {
                            "field": "arrondissement",
                            "interval": 1
                            }
                        }
                    }
                    }
                    
                }
        ).then(res =>callback(res.body.aggregations)).catch(err => console.error(err.meta.body.error));
}

exports.statsByType = (client, callback) => {
    // TODO Trouver le top 5 des types et sous types d'anomalies
    client.search({
        index: 'test_dans_ma_rue',
        body: {"size":0,
            "aggs": {
                "Type": {
                    "terms":{
                        "field": "type.keyword",
                        "size": 5
                    }, "aggs": {
                        "sous_types":{
                            "terms": {
                                "field": "sous_type.keyword",
                                "size":5
                            }
                        }
                    }
                        
                    }
                }
                }
                
            }
    ).then(res =>callback(res.body.aggregations)).catch(err => console.error(err.meta.body.error));   
}

exports.statsByMonth = (client, callback) => {
    // TODO Trouver le top 10 des mois avec le plus d'anomalies
    client.search({
        index: 'test_dans_ma_rue',
        body: {
            "aggs": {
                "result": {
                    "composite":{
                        "size": 10000,
                        "sources": [
                            {"month": {"terms": {"field" :"mois_declaration.keyword"}}},
                            {"year": {"terms":{ "field": "annee_declaration.keyword"}}}]
                    }
                        
                    }
                }
                }
                }
    ).then(res => {
        console.log(res.body.aggregations.result.buckets)
        callback(sort_show(res.body.aggregations.result.buckets))
    }
    ).catch(err => {
        console.error(err)
        callback([])
    });
    

}
function sort_show(buckets){
    return buckets.sort(function(a, b) {
        return parseFloat(b.doc_count) - parseFloat(a.doc_count);
    }).slice(0, 10);
    /*
    const result = [];
    while (result.length < 10){
        result.push(buckets.pop());
    }
    console.log(result);
    return result;
    */
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    client.search({
        index: 'test_dans_ma_rue',
        body:{
            "query": {
                "match":{
                    "type": {
                        "query":"Propreté"
                    }
                }
            },
            "aggs": {
                "Arrondissement": {
                    "terms": {
                        "field": "arrondissement", 
                        "size":3
                    }
                }
            }   
        }
    
    }).then(res =>callback(res.body)).catch(err => console.error(err.meta.body.error));   

}
