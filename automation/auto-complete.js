const datefns = require("date-fns");

schedule("foobar", () => {
	const grps = tasks.query({view: { filter: "completed:false status:Done", name: "automation"} })

	grps.groups[0].tasks
		.forEach(t => {
			tasks.complete({taskId: t.id})
		})
})
