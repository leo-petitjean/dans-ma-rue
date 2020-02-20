const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');

async function run () {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });
    client.indices.create({ index: 'test_dans_ma_rue', body: { mappings: {
        properties: {"location": {'type': 'geo_point'}, "arrondissement": {"type" : 'integer'}}} }}, (err, resp) => {
        if (err) console.trace(err.message);
      });
      
    // TODO il y a peut être des choses à faire ici avant de commencer ... 
    const bulk_Size = 10000;
    let bulk_anomalies = [];
    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            if (bulk_anomalies.length <= bulk_Size){
                bulk_anomalies.push({
                    "@timestamp" : data.DATEDECL,
                    "object_id" : data.OBJECTID,
                    "annee_declaration" : data["ANNEE DECLARATION"],
                    "mois_declaration" : data["MOIS DECLARATION"],
                    "type" : data.TYPE,
                    "sous_type" : data.SOUSTYPE,
                    "code_postal" : data.CODE_POSTAL,
                    "ville" : data.VILLE,
                    "arrondissement" : data.ARRONDISSEMENT,
                    "prefixe" : data.PREFIXE,
                    "intervenant" : data.INTERVENANT,
                    "conseil_de_quartier" : data.CONSEIL_DE_QUARTIER,
                    "location" : data.geo_point_2d
                })
            }
            if (bulk_anomalies.length >= bulk_Size){
                client.bulk(createBulkInsertQuery(bulk_anomalies), (err, resp) => {
                    if (err) console.trace(err.message);
                    else console.log(`Inserted ${resp.body.items.length} test_dans_ma_rue`);
                    client.close();
                  });
                bulk_anomalies =[]
                bulk_anomalies.push({
                    "@timestamp" : data.DATEDECL,
                    "object_id" : data.OBJECTID,
                    "annee_declaration" : data.ANNEE_DECLARATION,
                    "mois_declaration" : data.MOIS_DECLARATION,
                    "type" : data.TYPE,
                    "sous_type" : data.SOUSTYPE,
                    "code_postal" : data.CODE_POSTAL,
                    "ville" : data.VILLE,
                    "arrondissement" : data.ARRONDISSEMENT,
                    "prefixe" : data.PREFIXE,
                    "intervenant" : data.INTERVENANT,
                    "conseil_de_quartier" : data.CONSEIL_DE_QUARTIER,
                    "location" : data.geo_point_2d
                })
            }
            // TODO ici on récupère les lignes du CSV ...

            //console.log(data);
        })
        .on('end', () => {
            client.bulk(createBulkInsertQuery(bulk_anomalies), (err, resp) => {
                if (err) console.trace(err.message);
                else console.log(`Inserted ${resp.body.items.length} test_dans_ma_rue`);
                client.close();
              });
            // TODO il y a peut être des choses à faire à la fin aussi ?
            console.log('Terminated!');
        });
function createBulkInsertQuery(annomalies) {
    const body = annomalies.reduce((acc, annomalie) => {
        //const {timestamp, object_id, annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement,
        //prefixe, intervenant, conseil_de_quartier, location} = annomalie;
        acc.push({ index: { _index: 'test_dans_ma_rue', _type: '_doc', _id: annomalie.object_id } })
        acc.push(annomalie)
        return acc
    }, []);
    
    return { body };
    }
}

run().catch(console.error);
