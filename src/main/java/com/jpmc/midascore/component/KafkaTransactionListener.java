package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public KafkaTransactionListener(
            UserRepository userRepository,
            TransactionRecordRepository transactionRecordRepository
    ) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void listen(Transaction transaction) {

        UserRecord sender =
                userRepository.findById(transaction.getSenderId());

        UserRecord recipient =
                userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        sender.setBalance(
                sender.getBalance() - transaction.getAmount()
        );

        recipient.setBalance(
                recipient.getBalance() + transaction.getAmount()
        );

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord record =
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount()
                );

        transactionRecordRepository.save(record);

        System.out.println("Transaction processed");
    }
}