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


// function for adding a product to the 'product' table
// 'add' action gets logged with 'shop' set to empty string and 'quantity' set to 0
const addProduct = async (plu, name) => {
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        await client.query("INSERT INTO products (plu, name) VALUES ($1, $2)", [plu, name]);
        const response = await fetch("http://localhost:3001/log", {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                shop: '',
                plu: plu,
                action: 'add',
                quantity: 0
            })
        })
        if (!response.ok) {
            throw new Error('Failed to fetch API');
        }
        await client.query("COMMIT");
        return {
            success: true
        }
    } catch (err) {
        await client.query("ROLLBACK");
        console.log("DB error!", err);
        return {
            success: false
        }
    } finally {
        client.release();
    }
};


// function for getting a row from 'product' table by 'plu' or 'name' column value
// doesn't get logged
const getProduct = async (plu, name) => {
    let queryString = "SELECT * FROM products WHERE "
    if ((plu || name) && !(plu && name)) {
        const client = await pool.connect();
        try {
            let queryResult;
            if (plu) {
                queryString += "plu = $1";
                queryResult = await client.query(queryString, [plu]);
            } else {
                queryString += "name = $1";
                queryResult = await client.query(queryString, [name]);
            }
            return {
                success: true,
                rows: queryResult.rows
            }
        } catch (err) {
            console.log(err);
            return {
                success: false,
                error: "DB error!"
            }
        } finally {
            client.release();
        }
    } else {
        return {
            success: false,
            error: "Wrong parameters!"
        }
    }
}


// function for increasing and decreasing 'quantity_on_shelf' in stock table
const stockIncDec = async (action, id, value) => {
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        const stockRow = await client.query("SELECT * FROM stock WHERE id = $1", [id])
        const shop = stockRow.rows[0].shop;
        const productId = stockRow.rows[0].product_id;
        const plu = await client.query("SELECT plu FROM products WHERE id = $1", [productId])
        let queryString = "UPDATE stock SET quantity_on_shelf = quantity_on_shelf" + (action==="dec"?" - ":" + ") + "$1 WHERE id = $2";
        await client.query(queryString, [value, id]);
        const response = await fetch("http://localhost:3001/log", {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                shop: shop,
                plu: plu.rows[0].plu,
                action: (action==="dec"?"decrease":"increase"),
                quantity: value
            })
        })
        if (!response.ok) {
            throw new Error('Failed to fetch API');
        }
        await client.query("COMMIT");
        return {
            success: true
        }
    } catch (err) {
        await client.query("ROLLBACK");
        console.log("DB error!", err);
        return {
            success: false
        }
    } finally {
        client.release();
    }
}


// endpoint for adding a product to the 'product' table
app.post("/product", async (req, res)=>{
    const { plu, name} = req.body;
    const result = await addProduct(plu, name);
    if (result.success) {
        res.status(200).send("success");
    } else {
        res.status(500).send("failed");
    }    
});


// Endpoint for adding a row to the 'stock' table.
// Logging 'action_type' is set to stockSet, 'quantity' is set to the quantity on the shelf.
// Firstly, it will search existing row with given 'product_id' and 'shop' values, 
// if found 'quantity_on_shelf' and 'quantity_in_order' are altered, 
// if none found new row inserted
app.put("/stock/set", async (req, res)=>{
    const {plu, shop, quantityOnShelf, quantityInOrder} = req.body;
    if (!(plu && shop && quantityOnShelf && quantityInOrder)) {
        res.status(500).send("Wrong parameters!");
    }
    let productId;
    try {
        const productRow = await getProduct(plu);
        if (productRow.success) {
            productId = await productRow.rows[0].id;
        } else {
            throw new Error("failed plu query");
        }
    } catch (err) {
        console.log(err);
        res.status(500).send("Failed");
        return;
    }
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        const existingRow = await client.query(
            "SELECT * FROM stock WHERE product_id = $1 AND shop = $2", 
            [productId, shop]
        );
        if (existingRow.rows.length > 0) {
            await client.query(
                "UPDATE stock SET quantity_on_shelf = $1, quantity_in_order = $2 WHERE id = $3", 
                [quantityOnShelf, quantityInOrder, existingRow.rows[0].id]
            );
        } else {
            await client.query(
                "INSERT INTO stock (product_id, shop, quantity_on_shelf, quantity_in_order) VALUES ($1, $2, $3, $4)", 
                [productId, shop, quantityOnShelf, quantityInOrder]
            );
        }
        const response = await fetch("http://localhost:3001/log", {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                shop: shop,
                plu: plu,
                action: 'stockSet',
                quantity: quantityOnShelf
            })
        })
        if (!response.ok) {
            throw new Error('Failed to fetch API');
        }
        await client.query("COMMIT");
        res.status(200).send("Success");
    } catch (err) {
        await client.query("ROLLBACK");
        console.log("DB error!", err);
        res.status(500).send("Failed");
    } finally {
        client.release();
    }
});


// endpoint for increasing 'quantity_on_shelf' value in 'stock' table
app.patch("/stock/inc", async (req, res)=>{
    const {id, value} = req.query;
    const result = await stockIncDec("inc", id, value);
    if (result.success) {
        res.status(200).send("success");
    } else {
        res.status(500).send("failed");
    }   
});


// endpoint for decreasing 'quantity_on_shelf' value in 'stock' table
app.patch("/stock/dec", async (req, res)=>{
    const {id, value} = req.query;
    const result = await stockIncDec("dec", id, value);
    if (result.success) {
        res.status(200).send("success");
    } else {
        res.status(500).send("failed");
    }   
});


// - Получение остатков по фильтрам
// - plu
// - shop_id
// - количество остатков на полке (с-по)
// - количество остатков в заказе (с-по)
app.get("/stock", async (req, res)=>{
    const {plu, shop, shelfStock, orderStock} = req.body;
    let queryString = "SELECT * FROM stock WHERE ";
    let values = [];
    let filters = [];
    let index = 0;
    if (plu) {
        try {
            const productRow = await getProduct(plu);
            if (productRow.success) {
                const productId = await productRow.rows[0].id;
                
                index++;
                filters.push("product_id = $" + index);
                values.push(productId);
            } else {
                throw new Error("failed plu query");
            }
        } catch (err) {
            console.log(err);
            res.status(500).send("Failed");
            return;
        }
    }
    if (shop) {
        index++;
        filters.push("shop = $" + index);
        values.push(shop);
    }
    if (shelfStock) {
        if (shelfStock[0]) {
            index++;
            filters.push("quantity_on_shelf >= $" + index);
            values.push(shelfStock[0]);
        }
        if (shelfStock[1]) {
            index++;
            filters.push("quantity_on_shelf <= $" + index);
            values.push(shelfStock[1]);
        }
    }
    if (orderStock) {
        if (orderStock[0]) {
            index++;
            filters.push("quantity_in_order >= $" + index);
            values.push(orderStock[0]);
        }
        if (orderStock[1]) {
            index++;
            filters.push("quantity_in_order <= $" + index);
            values.push(orderStock[1]);
        }
    }
    queryString += filters.join(" AND ");
    const client = await pool.connect();
    try {
        const result = await client.query(queryString, values);
        console.log(result);
        console.log(result.rows);
        res.status(200).send(JSON.stringify(result.rows));
    } catch (error) {
        console.log("DB error!", err);
        res.status(500).send("Failed");
    } finally {
        client.release();
    }
});


// endpoint for getting a row from 'product' table by 'plu' or 'name' column value
app.get("/product", async (req, res)=>{
    const {plu, name} = req.query;
    const result = await getProduct(plu, name);
    if (result.success) {
        res.status(200).send(result.rows);
    } else {
        res.status(500).send(result.error);
    }
})


app.listen(3000, ()=>console.log("running"));


process.on('exit', () => {
    pool.end();
});