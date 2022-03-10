const axios = require('axios')
var xml2js = require('xml2js');
const mongoose = require("mongoose");

const username = "jorgeazorin";
const password = "Ba2xKCO6CPWZXZWQ";

mongoose.connect(
    //`mongodb://${username}:${password}@127.0.0.1/catast?retryWrites=true&w=majority`, 
    `mongodb+srv://${username}:${password}@cluster0.0crqf.mongodb.net/catast`,
    {
        useNewUrlParser: true,
        useUnifiedTopology: true
    }
);

const db = mongoose.connection;
db.on("error", console.error.bind(console, "connection error: "));
db.once("open", function () {
    console.log("Connected successfully");
});

const DatosSchema = new mongoose.Schema({
    provincia: {
        type: String,
        required: true,
    },
    municipio: {
        type: String,
        required: true,
    },
    pol: {
        type: String,
        required: true,
    },
    par: {
        type: String,
        required: true,
    },
    data: {
        type: Object,
        required: false
    }
});
const Datos = mongoose.model("Datos", DatosSchema);
const proxy = {
    // `proxy` means the request actually goes to the server listening
    // on localhost:3000, but the request says it is meant for
    // 'http://httpbin.org/get?answer=42'
    proxy: {
      host: 'p.speedproxies.net',
      port: 31112,
      auth: {
        username: 'jazorkspeedproxies',
        password: '7xvXmnD0Ru5NWI4k'
      }
    }
};

for (let k = 0; k<40; k++) {
    new Promise(() => start(k))
}

let totalCount = 0;

async function start(k) {
    const provincias = await getProvincias();
    //console.log(provincias)
    while (provincias) {
        await new Promise(resolve => setTimeout(resolve, 10000/(k+1)));
        try {
            const provincia = getRandomItem(provincias);
            const municipios = await getMunicipios(provincia);
            for (let i = 0; i < municipios.length; i++) {
                const municipio = municipios[i];
                console.log("-----  munpar -----", { provincia, municipio });
                let lastPolFound = 1;
                for (let pol = 1; pol <= 200; pol++) {
                    if (pol - lastPolFound > 5) {
                        break;
                    }
                    let parErr = 0;
                    for (let par = 1; par <= 200; par++) {
                        try {
                            const dato = await findInBD({ provincia, municipio, pol, par });
                            if (!dato) {
                                console.log('total:', totalCount, "new", provincia, municipio, pol, par );
                                let data = null;
                                try {
                                    await new Promise(resolve => setTimeout(resolve, 500));
                                    data = await getPolPar(provincia, municipio, pol, par);

                                    if (!data || !data?.consulta_dnp?.bico) {
                                        parErr++;
                                        if (parErr > 5) {
                                            break;
                                        }
                                    } else {
                                        parErr = 0;
                                        lastPolFound = pol;
                                        totalCount++;
                                    }
                                    const dato = new Datos({ provincia, municipio, pol, par, data });
                                    dato.save();
                                } catch (error) {
                                    console.log("Ups hour")
                                    //await new Promise(resolve => setTimeout(resolve, 3600000));
                                }
                            } else {
                                parErr = 0;
                                lastPolFound = pol;
                            }
                        } catch (error) {
                            console.log(error)
                        }
                    }
                }
            }
        } catch (error) {
            console.log(error)
        }
    }
}

function findInBD({ provincia, municipio, pol, par }, action) {
    //console.log("buscar", {provincia, municipio, pol, par});
    return new Promise((resolve, reject) => {
        Datos.exists({ provincia, municipio, pol, par }, (err, doc) => {
            //console.log("buscado", {provincia, municipio, pol, par})
            if (err) {
                console.log(err)
            } else {
                resolve(doc);
            }
        });
    });
}

async function getProvincias() {
    return await axios
        .get('http://ovc.catastro.meh.es/ovcservweb/OVCSWLocalizacionRC/OVCCallejero.asmx/ConsultaProvincia',proxy)
        .then(res => xml2js.parseStringPromise(res.data))
        .then(result => result.consulta_provinciero.provinciero[0].prov.map(t => t.np[0]))
        .catch(console.error);
}

async function getMunicipios(provincia) {
    return await axios
        .get(encodeURI(`http://ovc.catastro.meh.es/ovcservweb/OVCSWLocalizacionRC/OVCCallejero.asmx/ConsultaMunicipio?Provincia=${provincia}&Municipio=`), proxy)
        .then(res => xml2js.parseStringPromise(res.data))
        .then(result => result.consulta_municipiero.municipiero[0].muni.map(t => t.nm[0]))
        .catch(error => console.error(error, `http://ovc.catastro.meh.es/ovcservweb/OVCSWLocalizacionRC/OVCCallejero.asmx/ConsultaMunicipio?Provincia=${provincia}&Municipio=`), proxy);
}

async function getPolPar(provincia, municipio, pol, par) {
    return await axios
        .get(encodeURI(`http://ovc.catastro.meh.es//ovcservweb/OVCSWLocalizacionRC/OVCCallejero.asmx/Consulta_DNPPP?Provincia=${provincia}&Municipio=${municipio}&Poligono=${pol}&Parcela=${par}`), proxy)
        .then(res => xml2js.parseStringPromise(res.data))
}

// get random item from a Set
function getRandomItem(set) {
    let items = Array.from(set);
    return items[Math.floor(Math.random() * items.length)];
}

const delay = ms => new Promise(res => setTimeout(res, ms));
