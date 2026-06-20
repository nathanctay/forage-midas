package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private final RestTemplate restTemplate = new RestTemplate();

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {

        Optional<UserRecord> recipientOpt = userRepository.findById(transaction.getRecipientId());
        Optional<UserRecord> senderOpt = userRepository.findById(transaction.getSenderId());

        if (senderOpt.isEmpty()) {
            logger.warn("Invalid sender: {}", transaction.getSenderId());
            return;
        }
        if (recipientOpt.isEmpty()) {
            logger.warn("Invalid recipient: {}", transaction.getRecipientId());
            return;
        }

        UserRecord recipient = recipientOpt.get();
        UserRecord sender = senderOpt.get();

        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance. ");
            return;
        }

        try {
            Incentive incentive = restTemplate.postForObject(
                    "http://localhost:8080/incentive",
                    transaction,
                    Incentive.class
            );
            
            //set balances
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentive.getAmount());

            // save balances
            userRepository.save(sender);
            userRepository.save(recipient);

            // save records
            TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
            transactionRepository.save(transactionRecord);

            logger.info("Completed transaction: {}", transaction);
        } catch (Exception e) {
            logger.error("Something went wrong processing the transaction: {}", e.getMessage());
        }

    }
}
