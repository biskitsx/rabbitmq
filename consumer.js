const amqp = require("amqplib");
const mysql = require("mysql");

const connection = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "rootpassword",
  database: "orders",
});

connection.connect();

const sleep = (milliseconds) => {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
};

async function receiveOrders() {
// เชื่อมต่อกับ RabbitMQ
  const conn = await amqp.connect("amqp://mikelopster:password@localhost:5672");
    // สร้าง channel
  const channel = await conn.createChannel();

  const queue = "orders-new";
  await channel.assertQueue(queue, { durable: true });
  channel.prefetch(1); // รับ message ทีละ 1 ตัว

  let counter = 1;

  channel.consume(queue, async (msg) => {
    try {
        const order = JSON.parse(msg.content.toString());
        console.log(" [x] Received %s", order);
   
        await sleep(1000);
   
        const sql = "INSERT INTO orders SET ?";
        connection.query(sql, order, (error, results) => {
          if (error) throw error;
            console.log(`Order ${results.insertId} saved to database, countrer = ${counter}`);
            counter++;
            channel.ack(msg);
        });
    
          // บอกว่าได้ message แล้ว
        } catch (error) {
          console.log("Error:", error.message);
        }
      });
}

receiveOrders();
