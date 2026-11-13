import { greeting } from "./lib.js";
export default { fetch() { return new Response(greeting); } };
