package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/bwmarrin/discordgo"
)

// Bot parameters
var (
	GuildID        = flag.String("guild", "", "Test guild ID")
	BotToken       = flag.String("token", "", "Bot access token")
	AppID          = flag.String("app", "", "Application ID")
	Cleanup        = flag.Bool("cleanup", true, "Cleanup of commands")
	ResultsChannel = flag.String("results", "", "Channel where send survey results to")
)

var s *discordgo.Session

func init() {
	flag.Parse()
}

func init() {
	var err error
	s, err = discordgo.New("Bot " + *BotToken)
	if err != nil {
		log.Fatalf("Invalid bot parameters: %v", err)
	}
}

var (
	commands = []discordgo.ApplicationCommand{
		{
			Name:        "modals-survey",
			Description: "Take a survey about modals",
		},
	}
	commandsHandlers = map[string]func(s *discordgo.Session, i *discordgo.InteractionCreate){
		"modals-survey": func(s *discordgo.Session, i *discordgo.InteractionCreate) {
			err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
				Type: discordgo.InteractionResponseModal,
				Data: &discordgo.InteractionResponseData{
					CustomID: "modals_survey_" + i.Interaction.Member.User.ID,
					Title:    "Modals survey",
					Flags:    discordgo.MessageFlagsIsComponentsV2,
					Components: []discordgo.MessageComponent{
						discordgo.Label{
							Label:       "How would you rate them?",
							Description: "On a scale from terrible to awesome",
							Component: discordgo.SelectMenu{
								MenuType:    discordgo.StringSelectMenu,
								CustomID:    "rating",
								Placeholder: "Your rating...",
								Options: []discordgo.SelectMenuOption{
									{Label: "Terrible", Value: "terrible"},
									{Label: "Bad", Value: "bad"},
									{Label: "Neutral", Value: "neutral", Default: true},
									{Label: "Good", Value: "good"},
									{Label: "Awesome", Value: "awesome"},
								},
							},
						},
						discordgo.Label{
							Label:       "Which component do you like the most?",
							Description: "Pick exactly one",
							Component: discordgo.RadioGroup{
								CustomID: "favorite",
								Options: []discordgo.RadioGroupOption{
									{Label: "Buttons", Value: "buttons"},
									{Label: "Select menus", Value: "select_menus"},
									{Label: "Text inputs", Value: "text_inputs", Default: true},
								},
							},
						},
						discordgo.Label{
							Label:       "What do you use modals for?",
							Description: "Choose all that apply",
							Component: discordgo.CheckboxGroup{
								CustomID: "usages",
								Options: []discordgo.CheckboxGroupOption{
									{Label: "Moderation", Value: "moderation"},
									{Label: "Forms and surveys", Value: "forms"},
									{Label: "Games", Value: "games", Description: "Character creation, settings, etc."},
								},
								// Optional group: allow submitting without choosing anything.
								MinValues: new(int),
								Required:  new(bool),
							},
						},
						discordgo.Label{
							Label: "Would you recommend modals to a friend?",
							Component: discordgo.Checkbox{
								CustomID: "recommend",
								Default:  true,
							},
						},
						discordgo.Label{
							Label:       "What would you suggest to improve them?",
							Description: "Please provide as much info as possible!",
							Component: discordgo.TextInput{
								CustomID:  "suggestions",
								Style:     discordgo.TextInputParagraph,
								Required:  new(bool),
								MaxLength: 2000,
							},
						},
					},
				},
			})
			if err != nil {
				panic(err)
			}
		},
	}
)

func main() {
	s.AddHandler(func(s *discordgo.Session, r *discordgo.Ready) {
		log.Println("Bot is up!")
	})

	s.AddHandler(func(s *discordgo.Session, i *discordgo.InteractionCreate) {
		switch i.Type {
		case discordgo.InteractionApplicationCommand:
			if h, ok := commandsHandlers[i.ApplicationCommandData().Name]; ok {
				h(s, i)
			}
		case discordgo.InteractionModalSubmit:
			err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
				Type: discordgo.InteractionResponseChannelMessageWithSource,
				Data: &discordgo.InteractionResponseData{
					Content: "Thank you for taking your time to fill this survey",
					Flags:   discordgo.MessageFlagsEphemeral,
				},
			})
			if err != nil {
				panic(err)
			}
			data := i.ModalSubmitData()

			if !strings.HasPrefix(data.CustomID, "modals_survey") {
				return
			}

			userid := strings.Split(data.CustomID, "_")[2]

			// Radio groups return a single value, which is nil when the group is optional and left empty.
			favorite := "nothing"
			if v := data.Components[1].(*discordgo.Label).Component.(*discordgo.RadioGroup).Value; v != nil {
				favorite = *v
			}

			// Checkbox groups return every selected value, and an empty list when nothing is selected.
			usages := data.Components[2].(*discordgo.Label).Component.(*discordgo.CheckboxGroup).Values
			if len(usages) == 0 {
				usages = []string{"nothing"}
			}

			// Checkboxes return their state, which is nil only if the component was not submitted.
			recommend := false
			if v := data.Components[3].(*discordgo.Label).Component.(*discordgo.Checkbox).Value; v != nil {
				recommend = *v
			}

			_, err = s.ChannelMessageSend(*ResultsChannel, fmt.Sprintf(
				"Feedback received. From <@%s>\n\n**Rating**:\n%s\n\n**Favorite component**:\n%s\n\n**Used for**:\n%s\n\n**Recommends modals**:\n%t\n\n**Suggestions**:\n%s",
				userid,
				data.Components[0].(*discordgo.Label).Component.(*discordgo.SelectMenu).Values[0],
				favorite,
				strings.Join(usages, ", "),
				recommend,
				data.Components[4].(*discordgo.Label).Component.(*discordgo.TextInput).Value,
			))
			if err != nil {
				panic(err)
			}
		}
	})

	cmdIDs := make(map[string]string, len(commands))

	for _, cmd := range commands {
		rcmd, err := s.ApplicationCommandCreate(*AppID, *GuildID, &cmd)
		if err != nil {
			log.Fatalf("Cannot create slash command %q: %v", cmd.Name, err)
		}

		cmdIDs[rcmd.ID] = rcmd.Name
	}

	err := s.Open()
	if err != nil {
		log.Fatalf("Cannot open the session: %v", err)
	}
	defer s.Close()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	log.Println("Graceful shutdown")

	if !*Cleanup {
		return
	}

	for id, name := range cmdIDs {
		err := s.ApplicationCommandDelete(*AppID, *GuildID, id)
		if err != nil {
			log.Fatalf("Cannot delete slash command %q: %v", name, err)
		}
	}

}
