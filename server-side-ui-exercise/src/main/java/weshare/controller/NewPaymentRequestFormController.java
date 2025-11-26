package weshare.controller;

import io.javalin.http.Handler;
import weshare.model.Expense;
import weshare.model.Person;
import weshare.persistence.ExpenseDAO;
import weshare.server.ServiceRegistry;
import weshare.server.WeShareServer;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Map;

import static weshare.model.MoneyHelper.amountOf;

public class NewPaymentRequestFormController {
    private static String idGlo;
    public static final Handler view = context -> {
        ExpenseDAO expenseDAO = ServiceRegistry.lookup(ExpenseDAO.class);
        Person person = WeShareServer.getPersonLoggedIn(context);
        Collection<Expense> expenses = expenseDAO.findExpensesForPerson(person);

        String text = context.req.toString();
        String id = text.split("[=)]")[1];
        idGlo = id;

        Expense correctExpense = null;
        for (Expense expense : expenses) {
            String expenseId = String.valueOf(expense.getId());
            if (expenseId.equals(id)) {
                correctExpense = expense;
            }
        }

        Map<String, Object> viewModel = Map.of("paymentrequest",expenses,"correctExpense",correctExpense);
        context.render("paymentrequest.html", viewModel);
    };

    public static final Handler submit = context -> {
        ExpenseDAO expenseDAO = ServiceRegistry.lookup(ExpenseDAO.class);
        String id = idGlo;
        String email = context.formParamAsClass("email", String.class).get();
        Long amount = context.formParamAsClass("amount", Long.class).get();
        String dateString = context.formParamAsClass("due_date", String.class).get();

        String[] datesplit = dateString.split("-");
        int year = Integer.parseInt(datesplit[0]);
        int month = Integer.parseInt(datesplit[1]);
        int day = Integer.parseInt(datesplit[2]);
        LocalDate date = LocalDate.of(year, month, day);
        Person person = WeShareServer.getPersonLoggedIn(context);

        Collection<Expense> expenses = expenseDAO.findExpensesForPerson(person);
        for (Expense expense : expenses) {
            String expenseId = String.valueOf(expense.getId());
            if (expenseId.equals(id)) {
                expense.requestPayment(new Person(email),amountOf(amount),date);
                expenseDAO.save(expense);
            }
        }
        context.redirect("/paymentrequest?expenseId="+idGlo);
    };
}
