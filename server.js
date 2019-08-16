// call all the required packages
const express = require('express')
const bodyParser= require('body-parser')
var timeout = require('connect-timeout');
const multer = require('multer');
const app = express()
//const fs = require('fs');
//const middleware = require('./middleware/middlewares')
const middleware = require('./middleware/middle')

app.use((req, res, next) => {
  req.inject = {}
  res.header("Access-Control-Allow-Origin", "*")
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
  next()
})

app.use(bodyParser.urlencoded({extended: true}))
app.use(timeout(600000));
app.use(haltOnTimedout);

function haltOnTimedout(req, res, next){
  if (!req.timedout) next();
}

let storage = multer.diskStorage({
  destination: function (req, file, cb) {
      cb(null, 'uploads')
  },
  filename: function (req, file, cb) {
      cb(null, `__name__${file.originalname}`)
  }
})
let upload = multer({ storage: storage })

app.post('/uploadfile',
  upload.single('file'),
  (req, res, next) => {
    const file = req.file
    if (!file) {
      const error = new Error('Please upload a file')
      error.httpStatusCode = 400
      return res.send(error)
    }
    next()
  },
  //middleware.prepareFileNumber,
  middleware.parseFIle,
  middleware.uploadGCS
)

app.get('/flows', middleware.getJbs)


 
app.listen(3000, () => console.log('Server started on port 3000'));