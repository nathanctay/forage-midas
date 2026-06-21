package com.jpmc.midascore.controller;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;


@RestController
@RequestMapping("/balance")
public class BalanceController {
    @Autowired
    private UserRepository userRepository;

    @GetMapping
    Balance getBalance(@RequestParam long userId) {
        Optional<UserRecord> userOpt = userRepository.findById(userId);

        if (userOpt.isPresent()) {
            UserRecord user = userOpt.get();
            return new Balance(user.getBalance());
        }
        return new Balance(0);
    }
}
