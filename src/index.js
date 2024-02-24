const express =  require('express');
const cors = require("cors");
const bodyParser = require("body-parser")
const app = express();

const corsOptions = {
  origin: "http://localhost:3000" // frontend URI (ReactJS)
}

//middlewares
app.use(express.json());
app.use(express.urlencoded({extended:false}));
app.use(cors(corsOptions));

//routes
app.use(require('./routes/index'));

process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0;

app.listen(process.env.PORT||4000);

console.log('Server on port 4000');