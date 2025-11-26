package weshare.controller;

import io.javalin.http.Handler;
import weshare.model.Expense;
import weshare.model.Person;
import weshare.persistence.ExpenseDAO;
import weshare.server.ServiceRegistry;
import weshare.server.WeShareServer;

import java.util.Collection;
import java.util.Map;

public class NewExpenseFormController {
    public static final Handler view = context -> {
        ExpenseDAO expenseDAO = ServiceRegistry.lookup(ExpenseDAO.class);
        Person personLoggedIn= WeShareServer.getPersonLoggedIn(context);

        Collection<Expense> expenses = expenseDAO.findExpensesForPerson(personLoggedIn);
        Map<String, Object> viewModel = Map.of("newexpense",expenses);
        context.render("newexpense.html", viewModel);
    };
}
