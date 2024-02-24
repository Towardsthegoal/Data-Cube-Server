const { Router } = require("express");
const router = Router();
const express = require("express")

const {
  setDBInfo,
  mapData,
  queryCell,
  pivot,
  leaves
} = require("../controllers/index.controller");

router.post("/database", setDBInfo);

router.get("/test", (req, res) => {
  res.json({ msg: "Data  Cube Test is running" });
});

router.post("/mapData", mapData);

router.post("/queryCell", express.raw({ type: '*/*' }), queryCell);

router.post("/pivot", express.raw({ type: '*/*' }), pivot);

router.post("/leaves", leaves);

module.exports = router;
