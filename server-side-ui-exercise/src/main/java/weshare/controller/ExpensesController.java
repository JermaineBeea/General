package weshare.controller;

import io.javalin.http.Handler;
import org.javamoney.moneta.function.MonetaryFunctions;
import org.jetbrains.annotations.NotNull;
import weshare.model.Expense;
import weshare.model.Person;
import weshare.persistence.ExpenseDAO;
import weshare.server.ServiceRegistry;
import weshare.server.WeShareServer;

import javax.money.MonetaryAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static weshare.model.MoneyHelper.ZERO_RANDS;

public class ExpensesController {

    public static final Handler view = context -> {
        ExpenseDAO expensesDAO = ServiceRegistry.lookup(ExpenseDAO.class);
        Person personLoggedIn = WeShareServer.getPersonLoggedIn(context);

        Collection<Expense> expenses = expensesDAO.findExpensesForPerson(personLoggedIn);
        ArrayList<Expense> expenses1 = new ArrayList<>(expenses);
        double grandTotal = 0;

        for (Expense expense: expenses) {
            MonetaryAmount expensesAmount = expense.getAmount();
            double expenseValue = expensesAmount.getNumber().doubleValue();
            grandTotal += expenseValue;
        }

        expenses1.removeIf(Expense::isFullyPaidByOthers);

        String fomattedGrandTotal1 = String.format("ZAR %.2f", grandTotal);
        String formattedGrandTotal2 = fomattedGrandTotal1.split(",")[0];


        Map<String, Object> viewModel = Map.of("expenses", expenses1, "grandTotal", formattedGrandTotal2);
        context.render("expenses.html", viewModel);
    };
}
