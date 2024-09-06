require('dotenv').config();
const express = require("express");
const app = express();
app.use(express.json());
const { Pool } = require("pg");
const pool = new Pool({
    user: process.env.DB_USER,        
    host: process.env.DB_HOST,           
    database: process.env.DB_NAME, 
    password: process.env.DB_PASSWORD,    
    port: process.env.DB_PORT, 
});

// endpoin for logging
app.put("/log", async (req, res) => {
    const { shop, plu, action, quantity } = req.body;
    const client = await pool.connect();
    try {
        await client.query(
            "INSERT INTO logs (shop_id, plu, action_type, quantity) values ($1, $2, $3, $4)",
            [shop, plu, action, quantity]);
        res.status(200).send("Success");    
    } catch (err) {
        res.status(500).send("Failed");
    } finally {
        client.release();
    }
});


// endpoint for getting logs
app.get("/log", async (req, res) => {
    const {plu, shop, action, date} = req.body;
    const { page } = req.query;
    let queryString = "SELECT * FROM logs WHERE ";
    let filters = [];
    let values = [];
    let index = 0;
    if (plu) {
        index++;
        filters.push("plu = $" + index);
        values.push(plu);
    }
    if (shop) {
        index++;
        filters.push("shop_id = $" + index);
        values.push(shop);
    }
    if (action) {
        index++;
        filters.push("action_type = $" + index);
        values.push(action);
    }
    if (date) {
        if (date[0]) {
            index++;
            filters.push("action_date >= $" + index);
            values.push(date[0]);
        }
        if (date[1]) {
            index++;
            filters.push("action_date <= $" + index);
            values.push(date[1]);
        }
    }
    queryString += filters.join(" AND ");
    queryString +=  " ORDER BY id LIMIT 10 OFFSET " + (page-1)*10;
    const client = await pool.connect();
    try {
        const result = await client.query(queryString, values);
        res.status(200).send(JSON.stringify(result.rows));
    } catch (err) {
        res.status(500).send("Failed");
    } finally {
        client.release();
    }
});


app.listen(3001, ()=>{console.log("Logs running")});


process.on('exit', () => {
    pool.end();
});