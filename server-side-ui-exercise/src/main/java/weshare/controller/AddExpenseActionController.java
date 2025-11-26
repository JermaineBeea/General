package weshare.controller;


import io.javalin.http.Handler;
import weshare.model.Expense;
import weshare.model.Person;
import weshare.persistence.ExpenseDAO;
import weshare.persistence.PersonDAO;
import weshare.server.Routes;
import weshare.server.ServiceRegistry;
import weshare.server.WeShareServer;

import java.time.LocalDate;

import static weshare.model.MoneyHelper.amountOf;

public class AddExpenseActionController {
    public static final Handler submit = context -> {
        PersonDAO personDAO = ServiceRegistry.lookup(PersonDAO.class);
        ExpenseDAO expenseDAO = ServiceRegistry.lookup(ExpenseDAO.class);
        context.redirect(Routes.EXPENSES);

        String description = context.formParamAsClass("Description", String.class).get();
        long amount = context.formParamAsClass("Amount", long.class).get();
        String dateString = context.formParamAsClass("Date", String.class).get();

        String[] datesplit = dateString.split("-");
        int year = Integer.parseInt(datesplit[0]);
        int month = Integer.parseInt(datesplit[1]);
        int day = Integer.parseInt(datesplit[2]);
        LocalDate localDate = LocalDate.of(year, month, day);

        Person personLoggedIn = WeShareServer.getPersonLoggedIn(context);
        Expense expense = new Expense(personLoggedIn,description,amountOf(amount),LocalDate.now());
        expenseDAO.save(expense);
    };
}
