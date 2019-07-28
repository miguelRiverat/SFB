'strict'

const fs = require('fs')
const exec = require('child_process').exec
const lineReader = require('readline')
const { Storage } = require('@google-cloud/storage');
const projectId = 'prime-principle-243417';
const bucketName = 'incoming-files';
const gstorage = new Storage({
  projectId: projectId,
});
let months = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul','Ago','Sep','Oct','Nov', 'Dic']
let pesosIndex = []
let unidadesIndex = []
const parseDate = str => {
  let year = str.match(/[0-9]{2}/)
  let month = months.findIndex( (mth) => str.includes(mth))
  if (!year) { return false }
    year = year[0]
    let date = new Date()
    date.setFullYear(`20${year}`)
    date.setMonth(month)
    date.setDate(`01`)
    date.setHours(-5)
    return date.toISOString().split('T')[0]
}

const validDate = str => {
  if (!/[0-9]{6}/.test(str)) { return "" }
  return  `${str.slice(0,4)}-${str.slice(4,6)}-01`
}

const csvToArray = text => {
  let p = '', row = [''], ret = [row], i = 0, r = 0, s = !0, l;
  for (l of text) {
      if ('"' === l) {
          if (s && l === p) row[i] += l;
          s = !s;
      } else if (',' === l && s) l = row[++i] = '';
      else if ('\n' === l && s) {
          if ('\r' === p) row[i] = row[i].slice(0, -1);
          row = ret[++r] = [l = '']; i = 0;
      } else row[i] += l;
      p = l;
  }
  let retorno = ret[0] ? ret[0].map(elem => elem.includes(',') ? `"${elem}"` : elem) : []
  return [retorno]
}

let prepareFileNumber = (req, res, next) => {
  console.log('entre')
  const file = req.file
  let origin = file.path
  exec(`wc ${origin}`, function (error, results) {
    console.log('entre')
    if (error) {
      return res.send('badFile')
    }
    req.inject.lineNumber = results.split('   ')[1]
    next()
  })
}

let parseFIle = (req, res, next) => {
  console.log('enter')
  let exit = 0
  let lineNumber = Number(req.inject.lineNumber)
  const file = req.file
  let origin = file.path
  let target = `${file.path}.csv`
  let headers = []

  rl = lineReader.createInterface({
    input: fs.createReadStream(origin)
  })

  fs.writeFile(target, '', (err) => {
    if(err) { return console.log(err) }
  })
  console.log('enter')
  rl.on('line', (line) => {
    console.log('enter')
    if (exit+1 == lineNumber) { 
      req.inject.target = target
      next() 
    }
    if (exit === 0) {
      exit = exit + 1
      headers = csvToArray(line)[0]
      .filter(word => word !== ',' && word !== '')

      headers.forEach((head, index) => {
        if (head.includes('Pesos')) {
          pesosIndex.push(index)
        }
        if (head.includes('Uni')) {
          unidadesIndex.push(index)
        }
      })
      return
    }

    let lineSplited = csvToArray(line)[0]
    let productInfo = lineSplited.slice(0,14)
    productInfo[3] = validDate(productInfo[3])
    productInfo[4] = validDate(productInfo[4])
    let quantity = lineSplited.filter((lin, index) => unidadesIndex.includes(index))
    let price = lineSplited.filter((lin, index) => pesosIndex.includes(index))
    let stop = quantity.length > price.length 
      ? quantity.length
        : price.length 
    let dateNumber = 15
    for (let ii = 0 ; stop > ii; ii++) {
      try {
        let date = parseDate((headers[dateNumber] ? headers[dateNumber].toString() : '') || headers[ii])
        let operation = quantity[ii] / Number(price[ii].replace(/,/g,''))
        //console.log(quantity[ii], Number(price[ii].replace(/,/g,''), operation))
        fs.appendFileSync(target,`${productInfo.toString()},${date},${quantity[ii].replace(/,/g,'')},${price[ii].replace(/,/g,'')}\n` )
        dateNumber = dateNumber + 2
      } catch (err) {
        console.log(err)
      }
    }
    console.log(exit)
    exit = exit + 1
  })
}

let uploadGCS = (req, res) => {
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

module.exports = {
  prepareFileNumber,
  parseFIle,
  uploadGCS
}