var amqp = require('amqplib/callback_api');

var url2 = 'amqp://student:cph@datdb.cphbusiness.dk:5672';

amqp.connect(url2, function (err, conn) {
  conn.createChannel(function (err, ch) {
    var ex = 'group11RabbitBank.JSON';

    ch.assertExchange(ex, 'fanout', { durable: true });
    ch.checkExchange(ex, function (err, ok) {
        console.log('exchange running.');
      });

    console.log(' [*] Waiting for messages in %s. To exit press CTRL+C', ex);
    ch.consume(ex, function (msg) {
      console.log('in bank consume : ', msg.properties);
      var interest = calculateRate((JSON.parse(msg.content)).loanRequest.creditScore);
      var obj =
      {
        loanResponse:
        {
          interestRate: interest,
          ssn: (JSON.parse(msg.content)).loanRequest.ssn
        }
      };

      ch.bindQueue(msg.properties.replyTo, ex, '');
      ch.assertQueue(msg.properties.replyTo, { durable: true });
      ch.sendToQueue(msg.properties.replyTo,  new Buffer(JSON.stringify(obj)), { correlationId: msg.properties.correlationId});

    }, { noAck: false });
  });
});

function calculateRate(creditScore) {
  var interestRate;
  switch (true)
  {
  case (creditScore <= 100):
    interestRate = (Math.random() * 20);

  break;
  case (creditScore >= 200 && creditScore < 300):
    interestRate = (Math.random() * 16);

  break;
  case (creditScore >= 300 && creditScore < 400):
    interestRate = (Math.random() * 12);

  break;
  case (creditScore >= 400 && creditScore < 500):
    interestRate = (Math.random() * 8);
    console.log('4');
  break;
  case (creditScore >= 500 && creditScore < 600):
    interestRate = (Math.random() * 4);

  break;
  case (creditScore >= 600 && creditScore < 700):
    interestRate = (Math.random());

  break;
  case (creditScore >= 700 && creditScore < 800):
    interestRate = (Math.random() / 4);

  break;
  case (creditScore === 800):
    interestRate = (Math.random() / 8);

  break;

}

  return interestRate.toFixed(2);

};
