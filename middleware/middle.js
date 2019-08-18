'strict'

const fs = require('fs')
const { Storage } = require('@google-cloud/storage')
var jsonToCsv = require('convert-json-to-csv')
const csv = require('csvtojson')
const parse = require('csv-parser')
const write = require('csv-write-stream')
const projectId = 'prime-principle-243417';
const bucketName = 'incoming-files';

const gstorage = new Storage({
  projectId: projectId,
});
const {google} = require('googleapis');
const dataflow = google.dataflow('v1b3');

const validDate = str => {
  if (!/[0-9]{6}/.test(str)) { return "" }
  return  `${str.slice(0,4)}${str.slice(4,6)}01`
}

let parseFIle = (req, res, next) => {
  if (!req.body.date) { return res.status(400).jsonp({message: 'error en fecha'}) }
  let date = new Date(req.body.date)
  let year = date.getFullYear().toString()
  let month = date.getMonth()
  const file = req.file
  let origin = file.path
  let name = `${req.file.path}__`
  let target = `${name}`
  const csvFilePath = origin
  req.inject.target = target
  csv()
  .fromFile(csvFilePath)
  .then((jsonObj) => {
    let array = ["Clase Terapeutica Nivel3","Clase Terapeutica Nivel4","Corporacion","Fecha Lanzamiento Presentacion","Fecha Lanzamiento Producto","Forma Farmaceutica1","Forma Farmaceutica2","Forma Farmaceutica3","Genero","Laboratorio","Molecula N1","Presentacion","Producto","Punto de Venta"]
    let months = ["Ene","Feb","Mar", "Abr", "May", "Jun", "Jul", "Ago","Sep","Oct","Nov","Dic"]
    let newArray = jsonObj.reduce((acums, ele, index) => {
      let jsonProducts = array.reduce((retur, head) => {
        retur[head] = ele[head]
        return retur 
      },{})
      Object.keys(ele).forEach((headerKey, ind) => {
        if (headerKey.includes('MTH')) {
          Object.keys(ele[headerKey]).forEach(year => {
            jsonProducts[`${headerKey}${year}`] = ele[headerKey][year]
          })
        }
      })
      jsonProducts['Fecha Lanzamiento Presentacion'] = validDate(jsonProducts['Fecha Lanzamiento Presentacion'])
      jsonProducts["Fecha Lanzamiento Producto"] = validDate(jsonProducts['Fecha Lanzamiento Producto'])
      acums = acums.concat(jsonProducts)
      return acums
    },[])
    let arrayHeaders = Object.keys(newArray[0]).filter(head => head.includes('MTH'))
    let columnDef = ["Clase Terapeutica Nivel3","Clase Terapeutica Nivel4","Corporacion","Fecha Lanzamiento Presentacion","Fecha Lanzamiento Producto","Forma Farmaceutica1","Forma Farmaceutica2","Forma Farmaceutica3","Genero","Laboratorio","Molecula N1","Presentacion","Producto","Punto de Venta"].concat(arrayHeaders)
    //newArray.shift()
    var arrayOfObjectsCsv = jsonToCsv.convertArrayOfObjects(newArray, columnDef);
    fs.writeFile(target, arrayOfObjectsCsv, (err) => {
      if(err) { return console.log(err) }
    })
    next()
  })
}

let extraParse = (req, res, next) => {
  let name = `${req.file.path.replace("__name__", req.body.date.split("T")[0]+"__")}`
  let readStream =  fs.createReadStream(req.inject.target)
  req.inject.targetsep = name
  readStream
    .pipe(parse())
    .pipe(write({ separator: ';'}))
    .pipe(fs.createWriteStream(name))
  readStream.on('end', () => next())
}

let uploadGCS = (req, res) => {
  try{
    const file = req.file
    gstorage
      .bucket(bucketName)
      .upload(req.inject.targetsep, {name: req.inject.targetsep})
      .then(() => {
        console.log(`${req.inject.targetsep} uploaded to ${bucketName}.`);
        fs.unlinkSync(file.path)
        fs.unlinkSync(req.inject.target)
        fs.unlinkSync(req.inject.targetsep)
        return res.send(file)
      })
      .catch(err => {
        console.error('ERROR:', err);
        console.log(req.inject)
    
        return res.send(err) 
      })
  } catch (err) {console.log(err)}
}


let getJbs = (req, res, next) => {

  google.auth.getApplicationDefault(function (err, authClient, projectId) {
      if (err) {
          throw err;
      }
      if (authClient.createScopedRequired && authClient.createScopedRequired()) {
          authClient = authClient.createScoped([
              'https://www.googleapis.com/auth/compute'
          ]);
      }

      var compute = google.compute({
          version: 'v1',
          auth: authClient
      });

      var result = dataflow.projects.jobs.list({
          'projectId': projectId,
          'auth': authClient
      }, function (err, result) {
          console.log(err, result);
      })
  })

  

}

module.exports = {
  getJbs,
  parseFIle,
  uploadGCS,
  extraParse
}