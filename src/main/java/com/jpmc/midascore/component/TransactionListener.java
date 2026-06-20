package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class TransactionListener {
    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {

        Optional<UserRecord> recipientOpt = userRepository.findById(transaction.getRecipientId());
        Optional<UserRecord> senderOpt = userRepository.findById(transaction.getSenderId());
        if (recipientOpt.isPresent() && senderOpt.isPresent()) {
            UserRecord recipient = recipientOpt.get();
            UserRecord sender = senderOpt.get();

            if (sender.getBalance() > transaction.getAmount()) {
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance(recipient.getBalance() + transaction.getAmount());

                userRepository.save(sender);
                userRepository.save(recipient);

                TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
                transactionRepository.save(transactionRecord);

                System.out.println("Completed transaction: " + transaction);
            }
        }
    }
}
