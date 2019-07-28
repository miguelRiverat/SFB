'strict'

const fs = require('fs')
const { Storage } = require('@google-cloud/storage')
var jsonToCsv = require('convert-json-to-csv')
const csv = require('csvtojson')
const projectId = 'prime-principle-243417';
const bucketName = 'incoming-files';
const gstorage = new Storage({
  projectId: projectId,
});
const {google} = require('googleapis');
const dataflow = google.dataflow('v1b3');

const validDate = str => {
  if (!/[0-9]{6}/.test(str)) { return "" }
  return  `${str.slice(0,4)}-${str.slice(4,6)}-01`
}

let parseFIle = (req, res, next) => {
  const file = req.file
  let origin = file.path
  let target = `${file.path}.csv`
  const csvFilePath = origin
  req.inject.target = target
  csv()
  .fromFile(csvFilePath)
  .then((jsonObj) => {
    let array = ["Clase Terapeutica Nivel3","Clase Terapeutica Nivel4","Corporacion","Fecha Lanzamiento Presentacion","Fecha Lanzamiento Producto","Forma Farmaceutica1","Forma Farmaceutica2","Forma Farmaceutica3","Genero","Laboratorio","Molecula N1","Presentacion","Producto","Punto de Venta"]
    let months = ["Ene","Feb","Mar", "Abr", "May", "Jun", "Jul", "Ago","Sep","Oct","Nov","Dic"]
    let years = ["19", "18", "17", "16", "15"]

    let newArray = jsonObj.reduce((acums, ele, index) => {
      let jsonProducts = array.reduce((retur, head) => {
        retur[head] = ele[head]
        return retur 
      },{})
      jsonProducts['Fecha Lanzamiento Presentacion'] = validDate(jsonProducts['Fecha Lanzamiento Presentacion'])
      jsonProducts["Fecha Lanzamiento Producto"] = validDate(jsonProducts['Fecha Lanzamiento Producto'])
      let arrayToAdd = months.reduce((innerAcum, date, iidex) => {
        years.forEach(year => {
          let unidad = ele[`MTH Uni ${date}`][` ${year}`] 
          let pesos = ele[`MTH Pesos ${date}`][` ${year}`]
          if (pesos == 0 && unidad == 0) {
            let innerHead = Object.assign({}, jsonProducts)
            innerHead.unidad = 0
            innerHead.pesos = 0
            innerHead.operacion = 0
            let newDate = new Date()
            newDate.setFullYear(`20${year}`)
            newDate.setMonth(iidex)
            newDate.setDate(`01`)
            newDate.setHours(-5)
            innerHead.mes = newDate.toISOString().split('T')[0]
            innerAcum.push(innerHead)
          } else if (pesos != undefined && unidad != undefined) {
            let innerHead = Object.assign({}, jsonProducts)
            unidad = unidad.replace(/,/g,'')
            pesos = pesos.replace(/,/g,'')
            innerHead.unidad = unidad
            innerHead.pesos = pesos
            innerHead.operacion = Number(pesos)/Number(unidad)
            let newDate = new Date()
            newDate.setFullYear(`20${year}`)
            newDate.setMonth(iidex)
            newDate.setDate(`01`)
            newDate.setHours(-5)
            innerHead.mes = newDate.toISOString().split('T')[0]
            innerAcum.push(innerHead)
          }
        })
        return innerAcum
      },[])
      acums = acums.concat(arrayToAdd)
      if (index % 5000 === 0) {
        console.log(index)
      }
      return acums
    },[])
    let columnDef = ["Clase Terapeutica Nivel3","Clase Terapeutica Nivel4","Corporacion","Fecha Lanzamiento Presentacion","Fecha Lanzamiento Producto","Forma Farmaceutica1","Forma Farmaceutica2","Forma Farmaceutica3","Genero","Laboratorio","Molecula N1","Presentacion","Producto","Punto de Venta"].concat(["mes","unidad","pesos","operacion"])
    newArray.shift()
    var arrayOfObjectsCsv = jsonToCsv.convertArrayOfObjects(newArray, columnDef);
    fs.writeFile(target, arrayOfObjectsCsv, (err) => {
      if(err) { return console.log(err) }
    })
    next()
  })
}

let uploadGCS = (req, res) => {
  console.log('start')
  try{
    const file = req.file
    gstorage
      .bucket(bucketName)
      .upload(req.inject.target, {name: req.inject.target})
      .then(() => {
        console.log(`${req.inject.target} uploaded to ${bucketName}.`);
        fs.unlinkSync(file.path);
        //fs.unlinkSync(req.inject.target);
        res.send(file)
      })
      .catch(err => {
        console.error('ERROR:', err);
        res.send(err) 
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
  uploadGCS
}